DROP TABLE IF EXISTS origin.feature_partition_range;
CREATE TABLE origin.feature_partition_range(key text, value text, PRIMARY KEY (key));
INSERT INTO origin.feature_partition_range(key,value) VALUES ('key1','valueA');
INSERT INTO origin.feature_partition_range(key,value) VALUES ('key2','valueB');
INSERT INTO origin.feature_partition_range(key,value) VALUES ('key3','valueC');

DROP TABLE IF EXISTS target.feature_partition_range;
CREATE TABLE target.feature_partition_range(key text, value text, PRIMARY KEY (key));
