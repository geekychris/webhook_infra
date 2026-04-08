package main

import (
	"context"
	"crypto/rand"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/example/event-gateway/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store provides persistence for subscribers and subscriptions.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store backed by the given connection pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// RunMigrations reads embedded .up.sql files in sorted order and executes them.
func (s *Store) RunMigrations(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("reading migration dir: %w", err)
	}

	// Collect only .up.sql files and sort them.
	var upFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		data, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}
		slog.Info("running migration", "file", name)
		if _, err := s.pool.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("executing migration %s: %w", name, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Subscribers
// ---------------------------------------------------------------------------

// CreateSubscriber inserts a new subscriber with an auto-generated signing key
// and returns the populated model.
func (s *Store) CreateSubscriber(ctx context.Context, sub *model.Subscriber) (*model.Subscriber, error) {
	// Generate a 32-byte signing key if one is not already set.
	if len(sub.SigningKey) == 0 {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generating signing key: %w", err)
		}
		sub.SigningKey = key
	}

	// If a custom ID is provided, use it. Otherwise let the DB generate one.
	if sub.ID != "" {
		row := s.pool.QueryRow(ctx,
			`INSERT INTO subscribers (id, name, webhook_url, signing_key, stream_auth)
			 VALUES ($1, $2, $3, $4, $5)
			 RETURNING id, name, webhook_url, signing_key, stream_auth, created_at, updated_at`,
			string(sub.ID), sub.Name, sub.WebhookURL, sub.SigningKey, sub.StreamAuth,
		)
		return scanSubscriber(row)
	}
	row := s.pool.QueryRow(ctx,
		`INSERT INTO subscribers (name, webhook_url, signing_key, stream_auth)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, name, webhook_url, signing_key, stream_auth, created_at, updated_at`,
		sub.Name, sub.WebhookURL, sub.SigningKey, sub.StreamAuth,
	)
	return scanSubscriber(row)
}

// GetSubscriber returns a subscriber by ID.
func (s *Store) GetSubscriber(ctx context.Context, id model.SubscriberID) (*model.Subscriber, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, name, webhook_url, signing_key, stream_auth, created_at, updated_at
		 FROM subscribers WHERE id = $1`, id,
	)
	return scanSubscriber(row)
}

// ListSubscribers returns subscribers ordered by created_at with pagination.
func (s *Store) ListSubscribers(ctx context.Context, offset, limit int) ([]model.Subscriber, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, webhook_url, signing_key, stream_auth, created_at, updated_at
		 FROM subscribers ORDER BY created_at LIMIT $1 OFFSET $2`, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("querying subscribers: %w", err)
	}
	defer rows.Close()

	var out []model.Subscriber
	for rows.Next() {
		var sub model.Subscriber
		if err := rows.Scan(&sub.ID, &sub.Name, &sub.WebhookURL, &sub.SigningKey, &sub.StreamAuth, &sub.CreatedAt, &sub.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning subscriber: %w", err)
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// UpdateSubscriber updates the mutable fields of a subscriber.
func (s *Store) UpdateSubscriber(ctx context.Context, sub *model.Subscriber) (*model.Subscriber, error) {
	row := s.pool.QueryRow(ctx,
		`UPDATE subscribers
		 SET name = $2, webhook_url = $3, signing_key = $4, stream_auth = $5, updated_at = now()
		 WHERE id = $1
		 RETURNING id, name, webhook_url, signing_key, stream_auth, created_at, updated_at`,
		sub.ID, sub.Name, sub.WebhookURL, sub.SigningKey, sub.StreamAuth,
	)
	return scanSubscriber(row)
}

// DeleteSubscriber deletes a subscriber (cascading to subscriptions).
func (s *Store) DeleteSubscriber(ctx context.Context, id model.SubscriberID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM subscribers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting subscriber: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func scanSubscriber(row pgx.Row) (*model.Subscriber, error) {
	var sub model.Subscriber
	err := row.Scan(&sub.ID, &sub.Name, &sub.WebhookURL, &sub.SigningKey, &sub.StreamAuth, &sub.CreatedAt, &sub.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanning subscriber: %w", err)
	}
	return &sub, nil
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

// CreateSubscription inserts a new subscription. If sub.ID is set, uses that
// as the ID; otherwise the database generates one.
func (s *Store) CreateSubscription(ctx context.Context, sub *model.Subscription) (*model.Subscription, error) {
	if sub.ID != "" {
		row := s.pool.QueryRow(ctx,
			`INSERT INTO subscriptions
			 (id, subscriber_id, event_types, source_filter, tenant_filter, delivery_mode,
			  max_batch_size, max_batch_bytes, linger_ms, active)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 RETURNING id, subscriber_id, event_types, source_filter, tenant_filter,
			           delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
			           created_at, updated_at`,
			string(sub.ID), sub.SubscriberID, sub.EventTypes, sub.SourceFilter,
			tenantIDsToStrings(sub.TenantFilter), string(sub.DeliveryMode),
			sub.MaxBatchSize, sub.MaxBatchBytes, sub.LingerMS, sub.Active,
		)
		return scanSubscription(row)
	}
	row := s.pool.QueryRow(ctx,
		`INSERT INTO subscriptions
		 (subscriber_id, event_types, source_filter, tenant_filter, delivery_mode,
		  max_batch_size, max_batch_bytes, linger_ms, active)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING id, subscriber_id, event_types, source_filter, tenant_filter,
		           delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
		           created_at, updated_at`,
		sub.SubscriberID, sub.EventTypes, sub.SourceFilter,
		tenantIDsToStrings(sub.TenantFilter), string(sub.DeliveryMode),
		sub.MaxBatchSize, sub.MaxBatchBytes, sub.LingerMS, sub.Active,
	)
	return scanSubscription(row)
}

// GetSubscription returns a subscription by ID.
func (s *Store) GetSubscription(ctx context.Context, id model.SubscriptionID) (*model.Subscription, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, subscriber_id, event_types, source_filter, tenant_filter,
		        delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
		        created_at, updated_at
		 FROM subscriptions WHERE id = $1`, id,
	)
	return scanSubscription(row)
}

// ListSubscriptions returns subscriptions for a subscriber with pagination.
func (s *Store) ListSubscriptions(ctx context.Context, subscriberID model.SubscriberID, offset, limit int) ([]model.Subscription, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, subscriber_id, event_types, source_filter, tenant_filter,
		        delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
		        created_at, updated_at
		 FROM subscriptions WHERE subscriber_id = $1 ORDER BY created_at LIMIT $2 OFFSET $3`,
		subscriberID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("querying subscriptions: %w", err)
	}
	defer rows.Close()

	var out []model.Subscription
	for rows.Next() {
		sub, err := scanSubscriptionFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sub)
	}
	return out, rows.Err()
}

// UpdateSubscription updates the mutable fields of a subscription.
func (s *Store) UpdateSubscription(ctx context.Context, sub *model.Subscription) (*model.Subscription, error) {
	row := s.pool.QueryRow(ctx,
		`UPDATE subscriptions
		 SET event_types = $2, source_filter = $3, tenant_filter = $4, delivery_mode = $5,
		     max_batch_size = $6, max_batch_bytes = $7, linger_ms = $8, active = $9,
		     updated_at = now()
		 WHERE id = $1
		 RETURNING id, subscriber_id, event_types, source_filter, tenant_filter,
		           delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
		           created_at, updated_at`,
		sub.ID,
		sub.EventTypes,
		sub.SourceFilter,
		tenantIDsToStrings(sub.TenantFilter),
		string(sub.DeliveryMode),
		sub.MaxBatchSize,
		sub.MaxBatchBytes,
		sub.LingerMS,
		sub.Active,
	)
	return scanSubscription(row)
}

// DeleteSubscription deletes a subscription by ID.
func (s *Store) DeleteSubscription(ctx context.Context, id model.SubscriptionID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ListAllActive returns all active subscriptions across all subscribers.
// Used by the config distributor to build snapshots.
func (s *Store) ListAllActive(ctx context.Context) ([]model.Subscription, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, subscriber_id, event_types, source_filter, tenant_filter,
		        delivery_mode, max_batch_size, max_batch_bytes, linger_ms, active,
		        created_at, updated_at
		 FROM subscriptions WHERE active = true ORDER BY created_at`,
	)
	if err != nil {
		return nil, fmt.Errorf("querying active subscriptions: %w", err)
	}
	defer rows.Close()

	var out []model.Subscription
	for rows.Next() {
		sub, err := scanSubscriptionFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sub)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func scanSubscription(row pgx.Row) (*model.Subscription, error) {
	var sub model.Subscription
	var deliveryMode string
	var tenantFilter []string
	var createdAt, updatedAt time.Time

	err := row.Scan(
		&sub.ID, &sub.SubscriberID, &sub.EventTypes, &sub.SourceFilter,
		&tenantFilter, &deliveryMode, &sub.MaxBatchSize, &sub.MaxBatchBytes,
		&sub.LingerMS, &sub.Active, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scanning subscription: %w", err)
	}
	sub.DeliveryMode = model.DeliveryMode(deliveryMode)
	sub.TenantFilter = stringsToTenantIDs(tenantFilter)
	sub.CreatedAt = createdAt
	sub.UpdatedAt = updatedAt
	return &sub, nil
}

func scanSubscriptionFromRows(rows pgx.Rows) (*model.Subscription, error) {
	var sub model.Subscription
	var deliveryMode string
	var tenantFilter []string
	var createdAt, updatedAt time.Time

	err := rows.Scan(
		&sub.ID, &sub.SubscriberID, &sub.EventTypes, &sub.SourceFilter,
		&tenantFilter, &deliveryMode, &sub.MaxBatchSize, &sub.MaxBatchBytes,
		&sub.LingerMS, &sub.Active, &createdAt, &updatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scanning subscription row: %w", err)
	}
	sub.DeliveryMode = model.DeliveryMode(deliveryMode)
	sub.TenantFilter = stringsToTenantIDs(tenantFilter)
	sub.CreatedAt = createdAt
	sub.UpdatedAt = updatedAt
	return &sub, nil
}

func tenantIDsToStrings(ids []model.TenantID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func stringsToTenantIDs(ss []string) []model.TenantID {
	out := make([]model.TenantID, len(ss))
	for i, s := range ss {
		out[i] = model.TenantID(s)
	}
	return out
}
