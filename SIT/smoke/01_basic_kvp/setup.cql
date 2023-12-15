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

DROP TABLE IF EXISTS origin.smoke_basic_kvp;
CREATE TABLE origin.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
INSERT INTO origin.smoke_basic_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO origin.smoke_basic_kvp(key,value) VALUES ('key2','valueB');

DROP TABLE IF EXISTS target.smoke_basic_kvp;
CREATE TABLE target.smoke_basic_kvp(key text, value text, PRIMARY KEY (key));
