DROP TABLE IF EXISTS origin.codec;
CREATE TABLE origin.codec(key text, val_int text, val_bigint text, val_timestamp text, PRIMARY KEY (key));
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp) VALUES ('key1','1234'  ,'9223372036854775807','1087383600000');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp) VALUES ('key2','12345' ,'2147483648'         ,'1087384500000');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp) VALUES ('key3','123456','3141592653589793'   ,'1087385400000');
SELECT * FROM origin.codec;

DROP TABLE IF EXISTS target.codec;
CREATE TABLE target.codec(key text, val_int int, val_bigint bigint, val_timestamp timestamp, PRIMARY KEY (key));
