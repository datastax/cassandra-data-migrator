
DELETE FROM target.feature_constant_column WHERE key='key2' AND const1='abcd';
UPDATE target.feature_constant_column SET value='value999' WHERE key='key3' AND const1='abcd';
SELECT * FROM target.feature_constant_column;

