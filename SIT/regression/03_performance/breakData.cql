DELETE   FROM target.regression_performance WHERE pk_id = 600;
SELECT * FROM target.regression_performance WHERE pk_id >= 400 AND pk_id < 700 ALLOW FILTERING;
