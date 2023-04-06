DROP TABLE IF EXISTS origin.smoke_autocorrect_kvp;
CREATE TABLE origin.smoke_autocorrect_kvp(key text, value text, PRIMARY KEY (key));
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key2','valueB');
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key3','valueC');

DROP TABLE IF EXISTS target.smoke_autocorrect_kvp;
CREATE TABLE target.smoke_autocorrect_kvp(key text, value text, PRIMARY KEY (key));
