
DELETE FROM target.feature_explode_map_with_constants WHERE const1='abcd' AND const2=1234 AND key='key2';
UPDATE target.feature_explode_map_with_constants SET time=7398730800000 WHERE const1='abcd' AND const2=1234 AND key='key3' AND fruit='apples';
UPDATE target.feature_explode_map_with_constants SET fruit_qty=999      WHERE const1='abcd' AND const2=1234 AND key='key3' AND fruit='oranges';
DELETE FROM target.feature_explode_map_with_constants WHERE const1='abcd' AND const2=1234 AND key='key3' AND fruit='kiwi';
SELECT * FROM target.feature_explode_map_with_constants;
