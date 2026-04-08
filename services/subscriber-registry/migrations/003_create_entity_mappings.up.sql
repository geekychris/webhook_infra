CREATE TABLE IF NOT EXISTS entity_mappings (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    subscriber_id UUID NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id, subscriber_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_mappings_entity ON entity_mappings(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_mappings_subscriber ON entity_mappings(subscriber_id);
