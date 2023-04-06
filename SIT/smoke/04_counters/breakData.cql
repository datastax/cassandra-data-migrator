
UPDATE origin.smoke_counters SET col1=col1+1 WHERE key='record2';

-- CDM-29 discovered that this record will "zombie"...
-- fixData will not correct it, fixForce will
DELETE FROM target.smoke_counters where key='record3';

-- change by same amount on both sides, records should match
UPDATE origin.smoke_counters SET col2=col2+1 WHERE key='record4';
UPDATE target.smoke_counters SET col2=col2+1 WHERE key='record4';

-- change by different amounts, they shouuld be brought into alignment with 
-- origin, even if that means going down
UPDATE origin.smoke_counters SET col2=col2+1 WHERE key='record5';
UPDATE target.smoke_counters SET col2=col2+2 WHERE key='record5';

SELECT * FROM origin.smoke_counters;
SELECT * FROM target.smoke_counters;

