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

DELETE FROM target.map_columns WHERE key_a=2;
UPDATE target.map_columns SET val_a='valueD' WHERE key_a=3;

INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (1, 'key1','valueA', 21);
INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (2, 'key2','valueB', 22);
INSERT INTO origin.map_columns(key_a, key_b, val_a, val_b) VALUES (3, 'key3','valueC', 23);

SELECT * FROM target.map_columns;

