-- Add annotations field to subscriptions table

ALTER TABLE subscriptions
ADD COLUMN annotations jsonb DEFAULT '{}'::jsonb NOT NULL;

-- Add index on annotations for better query performance
CREATE INDEX subscriptions_annotations_idx ON subscriptions USING gin (annotations); 
