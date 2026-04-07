CREATE TABLE IF NOT EXISTS subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscriber_id UUID NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    event_types TEXT[] NOT NULL DEFAULT '{}',
    source_filter TEXT NOT NULL DEFAULT '*',
    tenant_filter TEXT[] NOT NULL DEFAULT '{}',
    delivery_mode TEXT NOT NULL DEFAULT 'webhook' CHECK (delivery_mode IN ('webhook', 'stream')),
    max_batch_size INT NOT NULL DEFAULT 100,
    max_batch_bytes INT NOT NULL DEFAULT 1048576,
    linger_ms INT NOT NULL DEFAULT 100,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_subscriber ON subscriptions(subscriber_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_active ON subscriptions(active) WHERE active = true;

CREATE OR REPLACE FUNCTION notify_subscription_change() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('subscription_changes', COALESCE(NEW.id, OLD.id)::text);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER subscription_change_trigger
    AFTER INSERT OR UPDATE OR DELETE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION notify_subscription_change();
