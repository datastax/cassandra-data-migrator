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

DELETE FROM target.feature_constant_column_remove WHERE key='key2';
UPDATE target.feature_constant_column_remove SET value='value999' WHERE key='key3';

# This upsert to origin will update the writetime on origin to be newer than target
INSERT INTO origin.feature_constant_column_remove(const1, key, value, const2) VALUES (1, 'key1','valueA', 21);
INSERT INTO origin.feature_constant_column_remove(const1, key, value, const2) VALUES (1, 'key2','valueB', 22);
INSERT INTO origin.feature_constant_column_remove(const1, key, value, const2) VALUES (1, 'key3','valueC', 23);

SELECT * FROM target.feature_constant_column_remove;
