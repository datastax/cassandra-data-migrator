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

DELETE FROM target.feature_explode_map WHERE key='key2';
UPDATE target.feature_explode_map SET value='value999' WHERE key='key3' AND fruit='apples';
UPDATE target.feature_explode_map SET fruit_qty=999 WHERE key='key3' AND fruit='oranges';
DELETE FROM target.feature_explode_map WHERE key='key3' AND fruit='kiwi';

# This upsert to origin will update the writetime on origin to be newer than target
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key1','valueA', {'apples': 3, 'oranges': 5, 'bananas': 2, 'grapes': 11});
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key2','valueB', {'apples': 4, 'oranges': 6, 'bananas': 3, 'pears': 7});
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key3','valueC', {'apples': 5, 'oranges': 7, 'bananas': 4, 'kiwi': 42});

SELECT * FROM target.feature_explode_map;

