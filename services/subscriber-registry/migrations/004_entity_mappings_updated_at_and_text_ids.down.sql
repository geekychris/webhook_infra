ALTER TABLE entity_mappings DROP COLUMN IF EXISTS updated_at;
ALTER TABLE entity_mappings ALTER COLUMN subscriber_id SET DATA TYPE UUID USING subscriber_id::uuid;
ALTER TABLE subscriptions ALTER COLUMN subscriber_id SET DATA TYPE UUID USING subscriber_id::uuid;
ALTER TABLE subscriptions ALTER COLUMN id SET DATA TYPE UUID USING id::uuid;
ALTER TABLE subscribers ALTER COLUMN id SET DATA TYPE UUID USING id::uuid;
