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

DROP TABLE IF EXISTS origin."ColumnRenameWithConstantsAndExplode";
CREATE TABLE origin."ColumnRenameWithConstantsAndExplode" (
    "key-name" text PRIMARY KEY,
    "fruit-map" map<text, int>
);

INSERT INTO origin."ColumnRenameWithConstantsAndExplode" ("key-name","fruit-map") VALUES ('key1', {'apples': 3, 'oranges': 5, 'bananas': 2, 'grapes': 11});
INSERT INTO origin."ColumnRenameWithConstantsAndExplode" ("key-name","fruit-map") VALUES ('key2', {'apples': 4, 'oranges': 6, 'bananas': 3, 'pears': 7});
INSERT INTO origin."ColumnRenameWithConstantsAndExplode" ("key-name","fruit-map") VALUES ('key3', {'apples': 5, 'oranges': 7, 'bananas': 4, 'kiwi': 42});

DROP TABLE IF EXISTS target.column_rename_with_constants_and_explode;
CREATE TABLE target.column_rename_with_constants_and_explode (
    customer text,
    year int,
    key_name text,
    fruit text,
    fruit_qty int,
    PRIMARY KEY ((customer, year, key_name), fruit)
);
