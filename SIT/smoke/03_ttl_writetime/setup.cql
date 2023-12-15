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

DROP TABLE IF EXISTS origin.smoke_ttl_writetime;
CREATE TABLE origin.smoke_ttl_writetime(key text, t_col1 text,tw_col2 text,w_col3 text,col4 text, PRIMARY KEY (key));

-- record1 : expect TTL≈50000 from A and writetime :02 1087383720000000 from C
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record1','AAAA') USING TTL  60000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record1','BBBB') USING TTL   5000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record1','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record1','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record2 : expect TTL≈50000 from B and writetime 02: 1087383720000000 from B
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record2','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record2','BBBB') USING TTL  60000 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record2','CCCC') USING TTL    400 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record2','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record3 : expect TTL≈50000 from B and writetime 02: 1087383720000000 from C
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record3','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record3','BBBB') USING TTL  60000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record3','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record3','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record4: this is to test missing record. we will delete target.record4 with a later timestamp 11:00:03 
--          and then create a new origin.record4 with an even later timestamp than that 11:10:00.
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record4','AAAA') USING TTL 4000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record4','BBBB') USING TTL 4000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record4','CCCC') USING TTL 4000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record4','DDDD') USING TTL 4000 AND TIMESTAMP 1087383600000000; -- 11:00:00

-- record5: this is record 3, but to test changed ttl
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record5','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record5','BBBB') USING TTL  60000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record5','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record5','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record6: this is record 3, but to test changed writetime
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record6','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record6','BBBB') USING TTL  60000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record6','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record6','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record7: this is record 3, but to test changing both ttl and writetime
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record7','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record7','BBBB') USING TTL  60000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record7','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record7','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03

-- record8: this is similar to record5, but we will change t_col1 but no writetime columns, which means the change will be lost
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record8','AAAA') USING TTL   5000 AND TIMESTAMP 1087383600000000; -- 11:00:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record8','BBBB') USING TTL  60000 AND TIMESTAMP 1087383660000000; -- 11:00:01
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record8','CCCC') USING TTL    400 AND TIMESTAMP 1087383720000000; -- 11:00:02
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record8','DDDD') USING TTL 700000 AND TIMESTAMP 1087383780000000; -- 11:00:03


SELECT key
      ,t_col1
      ,WRITETIME(t_col1) AS t_col1_wt
      ,TTL(t_col1) AS t_col1_ttl
      ,tw_col2
      ,WRITETIME(tw_col2) AS tw_col2_wt
      ,TTL(tw_col2) AS tw_col2_ttl
      ,w_col3
      ,WRITETIME(w_col3) AS w_col3_wt
      ,TTL(w_col3) AS w_col3_ttl
      ,col4
      ,WRITETIME(col4) AS col4_wt
      ,TTL(col4) AS col4_ttl
  FROM origin.smoke_ttl_writetime;

DROP TABLE IF EXISTS target.smoke_ttl_writetime;
-- this gc_grace_seconds is nuts but it's a single-node instance and we're trying to tombstone a deleted record immediately
CREATE TABLE target.smoke_ttl_writetime(key text, t_col1 text,tw_col2 text,w_col3 text,col4 text, PRIMARY KEY (key)) WITH gc_grace_seconds=1;
