DROP TABLE IF EXISTS origin.regression_performance;
CREATE TABLE origin.regression_performance (pk_id int, ck_id text, f1 text, PRIMARY KEY (pk_id, ck_id) ) WITH CLUSTERING ORDER BY (ck_id ASC);

DROP TABLE IF EXISTS target.regression_performance;
CREATE TABLE target.regression_performance (pk_id int, ck_id text, f1 text, PRIMARY KEY (pk_id, ck_id) ) WITH CLUSTERING ORDER BY (ck_id ASC);
