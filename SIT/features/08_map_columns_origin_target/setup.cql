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

DROP TABLE IF EXISTS origin.map_columns;
CREATE TABLE origin.map_columns(key_a int, key_b text, val_a text, val_b int, PRIMARY KEY (key_a, key_b));
INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (1, 'key1','valueA', 21);
INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (2, 'key2','valueB', 22);
INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (3, 'key3','valueC', 23);

DROP TABLE IF EXISTS target.map_columns;
CREATE TABLE target.map_columns(key_a int, val_a text, val_c int, PRIMARY KEY (key_a));
