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

DELETE FROM target.smoke_ttl_writetime USING TIMESTAMP 1087383780000000 WHERE key='record4'; -- 11:00:03
INSERT INTO origin.smoke_ttl_writetime(key, t_col1) VALUES ('record4','AAAA') USING TTL 4000 AND TIMESTAMP 1087384200000000; -- 11:10:00
INSERT INTO origin.smoke_ttl_writetime(key,tw_col2) VALUES ('record4','BBBB') USING TTL 4000 AND TIMESTAMP 1087384200000000; -- 11:10:00
INSERT INTO origin.smoke_ttl_writetime(key, w_col3) VALUES ('record4','CCCC') USING TTL 4000 AND TIMESTAMP 1087384200000000; -- 11:10:00
INSERT INTO origin.smoke_ttl_writetime(key,   col4) VALUES ('record4','DDDD') USING TTL 4000 AND TIMESTAMP 1087384200000000; -- 11:10:00


--  t_col1 is TTL only, so the TTL value should be picked by the diff, but not the writetime
INSERT INTO origin.smoke_ttl_writetime(key, t_col1)  VALUES ('record5','A---') USING TTL 700000 AND TIMESTAMP 1087384200000000; -- 11:10:00
INSERT INTO origin.smoke_ttl_writetime(key, tw_col2) VALUES ('record5','B---') USING TTL 700000 AND TIMESTAMP 1087384200000000; -- 11:10:00

--  t_col1 is TTL only. Its timestamp will update correctly, but the change will be written with previous timestamps
--    from tw_col2 and w_col3, which means the change will be lost
INSERT INTO origin.smoke_ttl_writetime(key, t_col1)  VALUES ('record8','A---') USING TTL 700000 AND TIMESTAMP 1087384200000000; -- 11:10:00


--  w_col3 is Writetime only, so the writetime value should be picked by the diff, but not the diff
INSERT INTO origin.smoke_ttl_writetime(key, w_col3)  VALUES ('record6','C---') USING TTL 700000 AND TIMESTAMP 1087384200000000; -- 11:10:00

-- tw_col2 is TTL and Writetime, so both changes should be propatated by the diff
INSERT INTO origin.smoke_ttl_writetime(key, tw_col2) VALUES ('record7','B---') USING TTL 700000 AND TIMESTAMP 1087384200000000; -- 11:10:00

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
  FROM target.smoke_ttl_writetime;

