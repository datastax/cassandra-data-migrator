DROP TABLE IF EXISTS origin.codec;
CREATE TABLE origin.codec(key text, val_int text, val_bigint text, val_timestamp text, val_decimal text, PRIMARY KEY (key));
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal) 
      VALUES ('key1','1234'  ,'9223372036854775807','040616110000', '3.14');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal) 
      VALUES ('key2','12345' ,'2147483648'         ,'990616110000', '4.14');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal) 
      VALUES ('key3','123456','3141592653589793'   ,'990616110000', '5.14');
SELECT * FROM origin.codec;

DROP TABLE IF EXISTS target.codec;
CREATE TABLE target.codec(key text, val_int int, val_bigint bigint, val_timestamp timestamp, val_decimal decimal, PRIMARY KEY (key));
