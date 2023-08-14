DROP TABLE IF EXISTS origin.smoke_counters;
CREATE TABLE origin.smoke_counters(key text,col1 counter,col2 counter, PRIMARY KEY (key));

UPDATE origin.smoke_counters set col1=col1+1, col2=col2+2 where key='record1';
UPDATE origin.smoke_counters set col1=col1+10, col2=col2+20 where key='record2';
UPDATE origin.smoke_counters set col1=col1+1000, col2=col2+2000 where key='record3';
UPDATE origin.smoke_counters set col1=col1+10000, col2=col2+20000 where key='record4';
UPDATE origin.smoke_counters set col1=col1+500, col2=col2+500 where key='record5';
UPDATE origin.smoke_counters set col1=col1+500 where key='record6';

DROP TABLE IF EXISTS target.smoke_counters;
CREATE TABLE target.smoke_counters(key text,col1 counter,col2 counter, PRIMARY KEY (key));

SELECT * FROM origin.smoke_counters;
