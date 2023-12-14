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

UPDATE origin.smoke_counters SET col1=col1+1 WHERE key='record2';

-- CDM-29 discovered that this record will "zombie"...
-- fixData will not correct it, fixForce will
DELETE FROM target.smoke_counters where key='record3';

-- change by same amount on both sides, records should match
UPDATE origin.smoke_counters SET col2=col2+1 WHERE key='record4';
UPDATE target.smoke_counters SET col2=col2+1 WHERE key='record4';

-- change by different amounts, they should be brought into alignment with
-- origin, even if that means going down
UPDATE origin.smoke_counters SET col2=col2+1 WHERE key='record5';
UPDATE target.smoke_counters SET col2=col2+2 WHERE key='record5';

SELECT * FROM origin.smoke_counters;
SELECT * FROM target.smoke_counters;

