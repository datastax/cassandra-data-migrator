DROP TABLE IF EXISTS source.feature_constant_column;
CREATE TABLE source.feature_constant_column(key text, value text, PRIMARY KEY (key));
INSERT INTO source.feature_constant_column(key,value) VALUES ('key1','valueA');
INSERT INTO source.feature_constant_column(key,value) VALUES ('key2','valueB');
INSERT INTO source.feature_constant_column(key,value) VALUES ('key3','valueC');

DROP TABLE IF EXISTS target.feature_constant_column;
CREATE TABLE target.feature_constant_column(const1 text, key text, value text, const2 int, PRIMARY KEY (const1, key));
