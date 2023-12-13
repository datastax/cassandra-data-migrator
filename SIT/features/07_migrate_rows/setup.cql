/*
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

DROP TABLE IF EXISTS origin.feature_migrate_rows;
CREATE TABLE origin.feature_migrate_rows(key text, key2 text, value text, PRIMARY KEY (key,key2));
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key1','A','valueA');
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key2','B','valueB');
INSERT INTO origin.feature_migrate_rows(key,key2,value) VALUES ('key3','C','valueC');

DROP TABLE IF EXISTS target.feature_migrate_rows;
CREATE TABLE target.feature_migrate_rows(key text, key2 text,value text, PRIMARY KEY (key,key2));
