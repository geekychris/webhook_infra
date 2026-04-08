//go:build !rocksdb

package main

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	DefaultTTL          = 30 * 24 * time.Hour
	timestampHeaderSize = 8
)

// PersistentQueueStore is a durable, compacting QueueStore backed by Pebble
// (pure-Go LSM engine). Used as the default when CGO/RocksDB is not available.
// Functionally identical to RocksDBQueueStore — same key layout, TTL, compaction.
type PersistentQueueStore struct {
	db     *pebble.DB
	ttl    time.Duration
	logger *slog.Logger

	subMu          sync.Map
	stopCompaction chan struct{}
}

// PersistentStoreOptions configures the Pebble queue store.
type PersistentStoreOptions struct {
	Path   string
	TTL    time.Duration
	Logger *slog.Logger
}

func NewPersistentQueueStore(opts PersistentStoreOptions) (*PersistentQueueStore, error) {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	db, err := pebble.Open(opts.Path, &pebble.Options{
		Levels: []pebble.LevelOptions{{Compression: pebble.SnappyCompression}},
	})
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	s := &PersistentQueueStore{
		db:             db,
		ttl:            opts.TTL,
		logger:         opts.Logger,
		stopCompaction: make(chan struct{}),
	}

	go s.compactionLoop()
	return s, nil
}

// --- Key encoding (identical to RocksDB store) ---

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

func queuePrefixEnd(subID string) []byte {
	p := queuePrefix(subID)
	end := make([]byte, len(p))
	copy(end, p)
	end[len(end)-1] = 1
	return end
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

func packValue(enqueuedAt int64, data []byte) []byte {
	val := make([]byte, timestampHeaderSize+len(data))
	binary.BigEndian.PutUint64(val, uint64(enqueuedAt))
	copy(val[timestampHeaderSize:], data)
	return val
}

func unpackValue(val []byte) (enqueuedAt int64, data []byte) {
	if len(val) < timestampHeaderSize {
		return 0, val
	}
	return int64(binary.BigEndian.Uint64(val[:timestampHeaderSize])), val[timestampHeaderSize:]
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

func (s *PersistentQueueStore) subLock(subID string) *sync.Mutex {
	v, _ := s.subMu.LoadOrStore(subID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *PersistentQueueStore) Append(subID string, events [][]byte) (uint64, error) {
	if len(events) == 0 {
		return 0, fmt.Errorf("no events to append")
	}

	mu := s.subLock(subID)
	mu.Lock()
	defer mu.Unlock()

	tail := s.readUint64(tailKey(subID))
	nextPos := tail + 1
	now := time.Now().UnixNano()

	batch := s.db.NewBatch()
	defer batch.Close()

	firstPos := nextPos
	for _, data := range events {
		batch.Set(queueKey(subID, nextPos), packValue(now, data), nil)
		nextPos++
	}
	batch.Set(tailKey(subID), encodeUint64(nextPos-1), nil)

	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("pebble commit: %w", err)
	}
	return firstPos, nil
}

func (s *PersistentQueueStore) Read(subID string, afterPos uint64, maxCount int) ([]StoreEntry, error) {
	startKey := queueKey(subID, afterPos+1)
	endKey := queuePrefixEnd(subID)

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: startKey, UpperBound: endKey})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	prefix := queuePrefix(subID)
	var entries []StoreEntry

	for iter.First(); iter.Valid() && len(entries) < maxCount; iter.Next() {
		key := iter.Key()
		if !hasPrefix(key, prefix) || len(key) < len(prefix)+8 {
			break
		}

		pos := binary.BigEndian.Uint64(key[len(prefix):])
		raw := make([]byte, len(iter.Value()))
		copy(raw, iter.Value())
		enqueuedAt, data := unpackValue(raw)

		entries = append(entries, StoreEntry{Position: pos, Data: data, EnqueuedAt: enqueuedAt})
	}
	return entries, nil
}

func (s *PersistentQueueStore) GetCursor(subID string) (uint64, error) {
	return s.readUint64(cursorKey(subID)), nil
}

func (s *PersistentQueueStore) SetCursor(subID string, pos uint64) error {
	return s.db.Set(cursorKey(subID), encodeUint64(pos), pebble.Sync)
}

func (s *PersistentQueueStore) GetTailPosition(subID string) (uint64, error) {
	return s.readUint64(tailKey(subID)), nil
}

func (s *PersistentQueueStore) ListSubscriptionsWithPending() ([]string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("t\x00"), UpperBound: []byte("t\x01"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var result []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 3 || key[0] != 't' || key[1] != 0 {
			continue
		}
		subID := string(key[2:])
		tail := decodeUint64(iter.Value())
		cursor := s.readUint64(cursorKey(subID))
		if tail > cursor {
			result = append(result, subID)
		}
	}
	return result, nil
}

func (s *PersistentQueueStore) Close() error {
	close(s.stopCompaction)
	return s.db.Close()
}

func (s *PersistentQueueStore) readUint64(key []byte) uint64 {
	val, closer, err := s.db.Get(key)
	if err != nil {
		return 0
	}
	defer closer.Close()
	return decodeUint64(val)
}

// --- Compaction ---

func (s *PersistentQueueStore) compactionLoop() {
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

func (s *PersistentQueueStore) runCompaction() {
	s.compactByCursor()
	s.compactByTTL()
}

func (s *PersistentQueueStore) compactByCursor() {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("c\x00"), UpperBound: []byte("c\x01"),
	})
	if err != nil {
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 3 {
			continue
		}
		subID := string(key[2:])
		cursor := decodeUint64(iter.Value())
		if cursor > 0 {
			s.db.DeleteRange(queueKey(subID, 0), queueKey(subID, cursor+1), pebble.NoSync)
		}
	}
}

func (s *PersistentQueueStore) compactByTTL() {
	cutoff := time.Now().Add(-s.ttl).UnixNano()

	tailIter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("t\x00"), UpperBound: []byte("t\x01"),
	})
	if err != nil {
		return
	}
	defer tailIter.Close()

	for tailIter.First(); tailIter.Valid(); tailIter.Next() {
		key := tailIter.Key()
		if len(key) < 3 {
			continue
		}
		subID := string(key[2:])
		s.compactSubByTTL(subID, cutoff)
	}
}

func (s *PersistentQueueStore) compactSubByTTL(subID string, cutoffNanos int64) {
	prefix := queuePrefix(subID)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: queueKey(subID, 0), UpperBound: queuePrefixEnd(subID),
	})
	if err != nil {
		return
	}
	defer iter.Close()

	var highestExpired uint64
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !hasPrefix(key, prefix) || len(key) < len(prefix)+8 {
			break
		}
		pos := binary.BigEndian.Uint64(key[len(prefix):])
		val := iter.Value()
		if len(val) >= timestampHeaderSize {
			enqueuedAt := int64(binary.BigEndian.Uint64(val[:timestampHeaderSize]))
			if enqueuedAt < cutoffNanos {
				highestExpired = pos
				continue
			}
		}
		break
	}

	if highestExpired > 0 {
		s.db.DeleteRange(queueKey(subID, 0), queueKey(subID, highestExpired+1), pebble.NoSync)
		cursor := s.readUint64(cursorKey(subID))
		if cursor < highestExpired {
			s.db.Set(cursorKey(subID), encodeUint64(highestExpired), pebble.Sync)
			s.logger.Info("TTL expired entries",
				"subscription_id", subID,
				"expired_through", highestExpired,
				"cursor_advanced_from", cursor,
			)
		}
	}
}
