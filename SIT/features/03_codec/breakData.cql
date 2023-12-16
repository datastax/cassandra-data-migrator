/*
 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/


DELETE FROM target.codec WHERE key='key2';
UPDATE target.codec SET 
       val_int=654321, 
       val_bigint=314159200000000, 
       val_timestamp='2024-04-16 10:30:00+0000', 
       val_decimal=999.1234
WHERE key='key3';

# This upsert to origin will update the writetime on origin to be newer than target
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal,val_double)
      VALUES ('key1','1234'  ,'9223372036854775807','040616110000', '3.14', '21474836470.7');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal,val_double)
      VALUES ('key2','12345' ,'2147483648'         ,'990616110000', '4.14', '21474836470.7');
INSERT INTO origin.codec(key,val_int,val_bigint,val_timestamp,val_decimal,val_double)
      VALUES ('key3','123456','3141592653589793'   ,'990616110000', '5.14', '21474836470.7');

SELECT * FROM target.codec;
