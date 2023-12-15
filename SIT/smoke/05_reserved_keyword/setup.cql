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

DROP TABLE IF EXISTS origin.smoke_order;
CREATE TABLE origin.smoke_order (col1 text primary key, "order" text);
INSERT INTO origin.smoke_order(col1, "order") VALUES ('c1','o1');
INSERT INTO origin.smoke_order(col1, "order") VALUES ('c2','o2');

DROP TABLE IF EXISTS target.smoke_order;
CREATE TABLE target.smoke_order (col1 text primary key, "order" text);


