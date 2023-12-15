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

DROP TABLE IF EXISTS origin.feature_explode_map;
CREATE TABLE origin.feature_explode_map(key text, value text, fruits map<text,int>, PRIMARY KEY (key));
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key1','valueA', {'apples': 3, 'oranges': 5, 'bananas': 2, 'grapes': 11});
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key2','valueB', {'apples': 4, 'oranges': 6, 'bananas': 3, 'pears': 7});
INSERT INTO origin.feature_explode_map(key,value,fruits) VALUES ('key3','valueC', {'apples': 5, 'oranges': 7, 'bananas': 4, 'kiwi': 42});

DROP TABLE IF EXISTS target.feature_explode_map;
CREATE TABLE target.feature_explode_map(key text, fruit text, value text, fruit_qty int, PRIMARY KEY ((key), fruit));
