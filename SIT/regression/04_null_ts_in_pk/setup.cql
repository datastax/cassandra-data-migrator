DROP TABLE IF EXISTS origin.regression_null_ts_in_pk;
CREATE TABLE origin.regression_null_ts_in_pk(key text, ts timestamp, value text, PRIMARY KEY (key));
INSERT INTO origin.regression_null_ts_in_pk(key,value) VALUES ('key1','valueA');
INSERT INTO origin.regression_null_ts_in_pk(key,ts,value) VALUES ('key2','2023-06-02 12:00:00','valueB');

DROP TABLE IF EXISTS target.regression_null_ts_in_pk;
CREATE TABLE target.regression_null_ts_in_pk(key text, ts timestamp, value text, PRIMARY KEY (key, ts));
