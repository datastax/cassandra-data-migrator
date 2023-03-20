
DELETE FROM target.smoke_autocorrect_kvp WHERE key='key2';
UPDATE target.smoke_autocorrect_kvp SET value='value999' WHERE key='key3';
SELECT * FROM target.smoke_autocorrect_kvp;

