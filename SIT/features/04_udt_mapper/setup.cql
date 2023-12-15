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

DROP TABLE IF EXISTS origin.udt_mapper;
DROP TYPE IF EXISTS origin.udt_mapper_origin;
CREATE TYPE origin.udt_mapper_origin (
    text_value text,
    long_value bigint,
    double_value double
);

CREATE TABLE origin.udt_mapper(
    key text, 
    val_udt udt_mapper_origin, 
    val_list list<frozen<udt_mapper_origin>>, 
    val_set set<frozen<udt_mapper_origin>>, 
    val_map map<text,frozen<udt_mapper_origin>>, 
  PRIMARY KEY (key));

INSERT INTO origin.udt_mapper (key, val_udt, val_list, val_set, val_map)
VALUES ('key1', 
                      {text_value: 'value10', long_value: 1000000000, double_value: 1.00}
       ,[             {text_value: 'value11', long_value: 1000000001, double_value: 1.01}
        ,             {text_value: 'value12', long_value: 1000000002, double_value: 1.02}]
       ,{             {text_value: 'value13', long_value: 1000000003, double_value: 1.03}
        ,             {text_value: 'value14', long_value: 1000000004, double_value: 1.04}}
        ,{'map_key1': {text_value: 'value15', long_value: 1000000005, double_value: 1.05}
         ,'map_key2': {text_value: 'value16', long_value: 1000000006, double_value: 1.06}});

INSERT INTO origin.udt_mapper (key, val_udt, val_list, val_set, val_map)
VALUES ('key2', 
                      {text_value: 'value20', long_value: 2000000000, double_value: 2.00}
       ,[             {text_value: 'value21', long_value: 2000000001, double_value: 2.01}
        ,             {text_value: 'value22', long_value: 2000000002, double_value: 2.02}]
       ,{             {text_value: 'value23', long_value: 2000000003, double_value: 2.03}
        ,             {text_value: 'value24', long_value: 2000000004, double_value: 2.04}}
        ,{'map_key1': {text_value: 'value25', long_value: 2000000005, double_value: 2.05}
         ,'map_key2': {text_value: 'value26', long_value: 2000000006, double_value: 2.06}});

INSERT INTO origin.udt_mapper (key, val_udt, val_list, val_set, val_map)
VALUES ('key3', 
                      {text_value: 'value30', long_value: 3000000000, double_value: 3.00}
       ,[             {text_value: 'value31', long_value: 3000000001, double_value: 3.01}
        ,             {text_value: 'value32', long_value: 3000000002, double_value: 3.02}]
       ,{             {text_value: 'value33', long_value: 3000000003, double_value: 3.03}
        ,             {text_value: 'value34', long_value: 3000000004, double_value: 3.04}}
        ,{'map_key1': {text_value: 'value35', long_value: 3000000005, double_value: 3.05}
         ,'map_key2': {text_value: 'value36', long_value: 3000000006, double_value: 3.06}});


SELECT * FROM origin.udt_mapper;

DROP TABLE IF EXISTS target.udt_mapper;
DROP TYPE IF EXISTS target.udt_mapper_target;
CREATE TYPE target.udt_mapper_target (
    text_value text,
    long_value bigint,
    double_value double
);

CREATE TABLE target.udt_mapper(
    key text, 
    val_udt udt_mapper_target, 
    val_list list<frozen<udt_mapper_target>>, 
    val_set set<frozen<udt_mapper_target>>, 
    val_map map<text,frozen<udt_mapper_target>>, 
  PRIMARY KEY (key));
