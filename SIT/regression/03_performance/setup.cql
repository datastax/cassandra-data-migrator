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

DROP TABLE IF EXISTS origin.regression_performance;
CREATE TABLE origin.regression_performance (pk_id int, ck_id text, f1 text, PRIMARY KEY (pk_id, ck_id) ) WITH CLUSTERING ORDER BY (ck_id ASC);

DROP TABLE IF EXISTS target.regression_performance;
CREATE TABLE target.regression_performance (pk_id int, ck_id text, f1 text, PRIMARY KEY (pk_id, ck_id) ) WITH CLUSTERING ORDER BY (ck_id ASC);
