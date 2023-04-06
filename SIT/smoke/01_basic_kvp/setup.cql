DROP TABLE IF EXISTS origin.smoke_basic_kvp;
CREATE TABLE origin.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
INSERT INTO origin.smoke_basic_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO origin.smoke_basic_kvp(key,value) VALUES ('key2','valueB');

DROP TABLE IF EXISTS target.smoke_basic_kvp;
CREATE TABLE target.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
