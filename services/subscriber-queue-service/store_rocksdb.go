//go:build cgo && rocksdb

package main

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

const (
	// DefaultTTL is the default retention period for queue entries.
	DefaultTTL = 30 * 24 * time.Hour // 30 days

	// timestampHeaderSize is the number of bytes prepended to each value
	// to store the enqueue timestamp (unix nanos, big-endian).
	timestampHeaderSize = 8
)

// RocksDBQueueStore is a durable, compacting QueueStore backed by RocksDB.
//
// Key design (all keys share the default column family):
//   - Queue data:  "q\x00" + <subID> + "\x00" + <8-byte big-endian position>
//                  Value: <8-byte enqueue timestamp nanos> + <event bytes>
//   - Cursors:     "c\x00" + <subID>  →  8-byte big-endian position
//   - Tails:       "t\x00" + <subID>  →  8-byte big-endian position
//
// Compaction runs every 30s and removes entries that are:
//   - At or below the acked cursor (delivered successfully), OR
//   - Older than the configured TTL (regardless of delivery status)
type RocksDBQueueStore struct {
	db     *grocksdb.DB
	ro     *grocksdb.ReadOptions
	wo     *grocksdb.WriteOptions
	ttl    time.Duration
	logger *slog.Logger

	subMu          sync.Map // map[string]*sync.Mutex
	stopCompaction chan struct{}
}

// RocksDBOptions configures the RocksDB queue store.
type RocksDBOptions struct {
	Path   string
	TTL    time.Duration // 0 = use DefaultTTL
	Logger *slog.Logger
}

// NewRocksDBQueueStore opens (or creates) a RocksDB database at the given path.
func NewRocksDBQueueStore(opts RocksDBOptions) (*RocksDBQueueStore, error) {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(128 << 20))

	dbOpts := grocksdb.NewDefaultOptions()
	dbOpts.SetBlockBasedTableFactory(bbto)
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCompression(grocksdb.LZ4Compression)
	dbOpts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(2))

	db, err := grocksdb.OpenDb(dbOpts, opts.Path)
	if err != nil {
		return nil, fmt.Errorf("open rocksdb: %w", err)
	}

	s := &RocksDBQueueStore{
		db:             db,
		ro:             grocksdb.NewDefaultReadOptions(),
		wo:             grocksdb.NewDefaultWriteOptions(),
		ttl:            opts.TTL,
		logger:         opts.Logger,
		stopCompaction: make(chan struct{}),
	}

	go s.compactionLoop()

	return s, nil
}

// --- Key encoding ---

func queueKey(subID string, pos uint64) []byte {
	key := make([]byte, 2+len(subID)+1+8)
	key[0] = 'q'
	key[1] = 0
	copy(key[2:], subID)
	key[2+len(subID)] = 0
	binary.BigEndian.PutUint64(key[2+len(subID)+1:], pos)
	return key
}

func queuePrefix(subID string) []byte {
	p := make([]byte, 2+len(subID)+1)
	p[0] = 'q'
	p[1] = 0
	copy(p[2:], subID)
	p[2+len(subID)] = 0
	return p
}

func cursorKey(subID string) []byte {
	k := make([]byte, 2+len(subID))
	k[0] = 'c'
	k[1] = 0
	copy(k[2:], subID)
	return k
}

func tailKey(subID string) []byte {
	k := make([]byte, 2+len(subID))
	k[0] = 't'
	k[1] = 0
	copy(k[2:], subID)
	return k
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// packValue prepends the enqueue timestamp to the event data.
func packValue(enqueuedAt int64, data []byte) []byte {
	val := make([]byte, timestampHeaderSize+len(data))
	binary.BigEndian.PutUint64(val, uint64(enqueuedAt))
	copy(val[timestampHeaderSize:], data)
	return val
}

// unpackValue splits the stored value into enqueue timestamp and event data.
func unpackValue(val []byte) (enqueuedAt int64, data []byte) {
	if len(val) < timestampHeaderSize {
		return 0, val
	}
	enqueuedAt = int64(binary.BigEndian.Uint64(val[:timestampHeaderSize]))
	data = val[timestampHeaderSize:]
	return
}

func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

// --- QueueStore interface ---

func (s *RocksDBQueueStore) subLock(subID string) *sync.Mutex {
	v, _ := s.subMu.LoadOrStore(subID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *RocksDBQueueStore) Append(subID string, events [][]byte) (uint64, error) {
	if len(events) == 0 {
		return 0, fmt.Errorf("no events to append")
	}

	mu := s.subLock(subID)
	mu.Lock()
	defer mu.Unlock()

	tail := s.readUint64(tailKey(subID))
	nextPos := tail + 1

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	now := time.Now().UnixNano()
	firstPos := nextPos
	for _, data := range events {
		batch.Put(queueKey(subID, nextPos), packValue(now, data))
		nextPos++
	}

	newTail := nextPos - 1
	batch.Put(tailKey(subID), encodeUint64(newTail))

	if err := s.db.Write(s.wo, batch); err != nil {
		return 0, fmt.Errorf("rocksdb write: %w", err)
	}

	return firstPos, nil
}

func (s *RocksDBQueueStore) Read(subID string, afterPos uint64, maxCount int) ([]StoreEntry, error) {
	prefix := queuePrefix(subID)
	startKey := queueKey(subID, afterPos+1)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := s.db.NewIterator(ro)
	defer iter.Close()

	var entries []StoreEntry
	iter.Seek(startKey)

	for i := 0; i < maxCount && iter.Valid(); i++ {
		key := iter.Key()
		keyData := key.Data()

		if !hasPrefix(keyData, prefix) || len(keyData) < len(prefix)+8 {
			key.Free()
			break
		}

		pos := binary.BigEndian.Uint64(keyData[len(prefix):])
		key.Free()

		val := iter.Value()
		rawVal := make([]byte, val.Size())
		copy(rawVal, val.Data())
		val.Free()

		enqueuedAt, data := unpackValue(rawVal)

		entries = append(entries, StoreEntry{
			Position:   pos,
			Data:       data,
			EnqueuedAt: enqueuedAt,
		})

		iter.Next()
	}

	return entries, nil
}

func (s *RocksDBQueueStore) GetCursor(subID string) (uint64, error) {
	return s.readUint64(cursorKey(subID)), nil
}

func (s *RocksDBQueueStore) SetCursor(subID string, pos uint64) error {
	return s.db.Put(s.wo, cursorKey(subID), encodeUint64(pos))
}

func (s *RocksDBQueueStore) GetTailPosition(subID string) (uint64, error) {
	return s.readUint64(tailKey(subID)), nil
}

func (s *RocksDBQueueStore) ListSubscriptionsWithPending() ([]string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := s.db.NewIterator(ro)
	defer iter.Close()

	tailPrefix := []byte("t\x00")
	var result []string

	iter.Seek(tailPrefix)
	for iter.Valid() {
		key := iter.Key()
		keyData := key.Data()
		if !hasPrefix(keyData, tailPrefix) {
			key.Free()
			break
		}
		subID := string(keyData[2:])
		key.Free()

		val := iter.Value()
		tail := decodeUint64(val.Data())
		val.Free()

		cursor := s.readUint64(cursorKey(subID))
		if tail > cursor {
			result = append(result, subID)
		}

		iter.Next()
	}

	return result, nil
}

func (s *RocksDBQueueStore) Close() error {
	close(s.stopCompaction)
	s.ro.Destroy()
	s.wo.Destroy()
	s.db.Close()
	return nil
}

// --- Helpers ---

func (s *RocksDBQueueStore) readUint64(key []byte) uint64 {
	val, err := s.db.Get(s.ro, key)
	if err != nil || !val.Exists() {
		if val != nil {
			val.Free()
		}
		return 0
	}
	defer val.Free()
	return decodeUint64(val.Data())
}

// --- Background compaction ---

func (s *RocksDBQueueStore) compactionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCompaction:
			return
		case <-ticker.C:
			s.runCompaction()
		}
	}
}

// runCompaction deletes events older than the TTL. Cursor-based compaction is
// disabled to preserve replay capability — streaming clients need to resume
// from earlier positions. TTL (default 30 days) provides the storage bound.
func (s *RocksDBQueueStore) runCompaction() {
	s.compactByTTL()
}

// compactByCursor deletes queue entries at or below the acked cursor.
func (s *RocksDBQueueStore) compactByCursor() {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := s.db.NewIterator(ro)
	defer iter.Close()

	cursorPrefix := []byte("c\x00")
	iter.Seek(cursorPrefix)

	for iter.Valid() {
		key := iter.Key()
		keyData := key.Data()
		if !hasPrefix(keyData, cursorPrefix) {
			key.Free()
			break
		}
		subID := string(keyData[2:])
		key.Free()

		val := iter.Value()
		cursor := decodeUint64(val.Data())
		val.Free()

		if cursor > 0 {
			batch := grocksdb.NewWriteBatch()
			batch.DeleteRange(queueKey(subID, 0), queueKey(subID, cursor+1))
			s.db.Write(s.wo, batch)
			batch.Destroy()
		}

		iter.Next()
	}
}

// compactByTTL scans all queue entries and deletes those older than the TTL.
// This catches entries from abandoned subscriptions or entries that were never
// delivered. It also advances the cursor past any TTL-expired entries so the
// dispatcher doesn't attempt to deliver stale events.
func (s *RocksDBQueueStore) compactByTTL() {
	cutoff := time.Now().Add(-s.ttl).UnixNano()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Iterate all tail keys to find subscriptions
	tailIter := s.db.NewIterator(ro)
	defer tailIter.Close()

	tailPrefix := []byte("t\x00")
	tailIter.Seek(tailPrefix)

	for tailIter.Valid() {
		key := tailIter.Key()
		keyData := key.Data()
		if !hasPrefix(keyData, tailPrefix) {
			key.Free()
			break
		}
		subID := string(keyData[2:])
		key.Free()
		tailIter.Value().Free()
		tailIter.Next()

		s.compactSubByTTL(subID, cutoff)
	}
}

// compactSubByTTL deletes expired entries for a single subscription.
func (s *RocksDBQueueStore) compactSubByTTL(subID string, cutoffNanos int64) {
	prefix := queuePrefix(subID)
	startKey := queueKey(subID, 0)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := s.db.NewIterator(ro)
	defer iter.Close()

	var highestExpired uint64
	iter.Seek(startKey)

	for iter.Valid() {
		key := iter.Key()
		keyData := key.Data()
		if !hasPrefix(keyData, prefix) || len(keyData) < len(prefix)+8 {
			key.Free()
			break
		}

		pos := binary.BigEndian.Uint64(keyData[len(prefix):])
		key.Free()

		val := iter.Value()
		rawVal := val.Data()
		if len(rawVal) >= timestampHeaderSize {
			enqueuedAt := int64(binary.BigEndian.Uint64(rawVal[:timestampHeaderSize]))
			if enqueuedAt < cutoffNanos {
				highestExpired = pos
				val.Free()
				iter.Next()
				continue
			}
		}
		val.Free()
		// First non-expired entry — stop scanning
		break
	}

	if highestExpired > 0 {
		// Delete all entries up to and including highestExpired
		batch := grocksdb.NewWriteBatch()
		batch.DeleteRange(queueKey(subID, 0), queueKey(subID, highestExpired+1))
		s.db.Write(s.wo, batch)
		batch.Destroy()

		// Advance cursor if it's behind the expired range, so the dispatcher
		// doesn't try to deliver events that no longer exist
		cursor := s.readUint64(cursorKey(subID))
		if cursor < highestExpired {
			s.db.Put(s.wo, cursorKey(subID), encodeUint64(highestExpired))
			s.logger.Info("TTL expired entries",
				"subscription_id", subID,
				"expired_through", highestExpired,
				"cursor_advanced_from", cursor,
			)
		}
	}
}

// --- Aliases so main.go compiles with either build tag ---

type PersistentStoreOptions = RocksDBOptions
type PersistentQueueStore = RocksDBQueueStore

func NewPersistentQueueStore(opts PersistentStoreOptions) (*PersistentQueueStore, error) {
	return NewRocksDBQueueStore(opts)
}
