  DELETE FROM target.column_rename_with_constants_and_explode                   WHERE customer='CUSTOMER' AND year=2023 AND key_name='key2';
       UPDATE target.column_rename_with_constants_and_explode SET fruit_qty=999 WHERE customer='CUSTOMER' AND year=2023 AND key_name='key3' AND fruit='oranges';
  DELETE FROM target.column_rename_with_constants_and_explode                   WHERE customer='CUSTOMER' AND year=2023 AND key_name='key3' AND fruit='kiwi';
SELECT * FROM target.column_rename_with_constants_and_explode;

