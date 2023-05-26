DROP TABLE IF EXISTS origin.feature_migrate_rows;
CREATE TABLE origin.feature_migrate_rows(key text, key2 text, value text, PRIMARY KEY (key,key2));
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key1','A','valueA');
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key2','B','valueB');
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key3','C','valueC');

DROP TABLE IF EXISTS target.feature_migrate_rows;
CREATE TABLE target.feature_migrate_rows(key text, key2 text,value text, PRIMARY KEY (key,key2));
