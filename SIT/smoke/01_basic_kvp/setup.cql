DROP TABLE IF EXISTS source.smoke_basic_kvp;
CREATE TABLE source.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
INSERT INTO source.smoke_basic_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO source.smoke_basic_kvp(key,value) VALUES ('key2','valueB');

DROP TABLE IF EXISTS target.smoke_basic_kvp;
CREATE TABLE target.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
