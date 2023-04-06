DROP TABLE IF EXISTS origin.smoke_test_harness;
CREATE TABLE origin.smoke_test_harness(key text, value text, PRIMARY KEY (key));
INSERT INTO origin.smoke_test_harness(key,value) VALUES ('key1','valueA');
INSERT INTO origin.smoke_test_harness(key,value) VALUES ('key2','valueB');

DROP TABLE IF EXISTS target.smoke_test_harness;
CREATE TABLE target.smoke_test_harness(key text, value text, PRIMARY KEY (key));
INSERT INTO target.smoke_test_harness(key,value) VALUES ('key1','valueA');
INSERT INTO target.smoke_test_harness(key,value) VALUES ('key2','valueB');
