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

DELETE FROM target.smoke_autocorrect_kvp WHERE key='key2';
UPDATE target.smoke_autocorrect_kvp SET value='value999' WHERE key='key3';

# This upsert to origin will update the writetime on origin to be newer than target
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key1','valueA');
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key2','valueB');
INSERT INTO origin.smoke_autocorrect_kvp(key,value) VALUES ('key3','valueC');

SELECT * FROM target.smoke_autocorrect_kvp;

