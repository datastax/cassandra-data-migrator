
DELETE FROM target.codec WHERE key='key2';
UPDATE target.codec SET 
       val_int=654321, 
       val_bigint=314159200000000, 
       val_timestamp='2024-04-16 10:30:00+0000', 
       val_decimal=999.1234
WHERE key='key3';
SELECT * FROM target.codec;
