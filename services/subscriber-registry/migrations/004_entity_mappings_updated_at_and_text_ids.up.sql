-- Add updated_at to entity_mappings (idempotent)
ALTER TABLE entity_mappings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Change IDs from UUID to TEXT to support arbitrary string formats.
-- This is idempotent: if columns are already TEXT, the ALTER is a no-op.
DO $$
BEGIN
    -- Drop foreign keys if they exist and reference UUID columns
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'subscribers' AND column_name = 'id' AND data_type = 'uuid'
    ) THEN
        ALTER TABLE entity_mappings DROP CONSTRAINT IF EXISTS entity_mappings_subscriber_id_fkey;
        ALTER TABLE subscriptions DROP CONSTRAINT IF EXISTS subscriptions_subscriber_id_fkey;

        ALTER TABLE subscribers ALTER COLUMN id SET DATA TYPE TEXT USING id::text;
        ALTER TABLE subscriptions ALTER COLUMN id SET DATA TYPE TEXT USING id::text;
        ALTER TABLE subscriptions ALTER COLUMN subscriber_id SET DATA TYPE TEXT USING subscriber_id::text;
        ALTER TABLE entity_mappings ALTER COLUMN subscriber_id SET DATA TYPE TEXT USING subscriber_id::text;

        ALTER TABLE subscriptions ADD CONSTRAINT subscriptions_subscriber_id_fkey
            FOREIGN KEY (subscriber_id) REFERENCES subscribers(id) ON DELETE CASCADE;
        ALTER TABLE entity_mappings ADD CONSTRAINT entity_mappings_subscriber_id_fkey
            FOREIGN KEY (subscriber_id) REFERENCES subscribers(id) ON DELETE CASCADE;
    END IF;
END $$;
