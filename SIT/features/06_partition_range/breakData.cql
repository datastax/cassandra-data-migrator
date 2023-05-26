
DELETE FROM target.feature_partition_range WHERE key='key1';
UPDATE target.feature_partition_range SET value='value999' WHERE key='key2';
SELECT * FROM target.feature_partition_range;

