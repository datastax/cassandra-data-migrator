DROP TABLE IF EXISTS source.smoke_autocorrect_kvp;
CREATE TABLE source.smoke_autocorrect_kvp(key text, value text, PRIMARY KEY (key));
INSERT INTO source.smoke_autocorrect_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO source.smoke_autocorrect_kvp(key,value) VALUES ('key2','valueB');
INSERT INTO source.smoke_autocorrect_kvp(key,value) VALUES ('key3','valueC');

DROP TABLE IF EXISTS target.smoke_autocorrect_kvp;
CREATE TABLE target.smoke_autocorrect_kvp(key text, value text, PRIMARY KEY (key));
