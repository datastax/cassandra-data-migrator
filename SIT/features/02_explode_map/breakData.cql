
DELETE FROM target.feature_explode_map WHERE key='key2';
UPDATE target.feature_explode_map SET value='value999' WHERE key='key3' AND fruit='apples';
UPDATE target.feature_explode_map SET fruit_qty=999 WHERE key='key3' AND fruit='oranges';
DELETE FROM target.feature_explode_map WHERE key='key3' AND fruit='kiwi';
SELECT * FROM target.feature_explode_map;

