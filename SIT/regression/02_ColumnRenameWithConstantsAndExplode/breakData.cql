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

DELETE FROM target.column_rename_with_constants_and_explode                   WHERE customer='CUSTOMER' AND year=2023 AND key_name='key2';
UPDATE target.column_rename_with_constants_and_explode SET fruit_qty=999 WHERE customer='CUSTOMER' AND year=2023 AND key_name='key3' AND fruit='oranges';
DELETE FROM target.column_rename_with_constants_and_explode                   WHERE customer='CUSTOMER' AND year=2023 AND key_name='key3' AND fruit='kiwi';
SELECT * FROM target.column_rename_with_constants_and_explode;

