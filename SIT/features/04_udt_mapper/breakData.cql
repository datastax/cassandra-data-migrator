DELETE FROM target.udt_mapper WHERE key='key2';
UPDATE target.udt_mapper SET
       val_udt  =              {text_value: 'value9990', long_value: 9990000000, double_value: 999.00}
      ,val_list = [            {text_value: 'value9991', long_value: 9990000001, double_value: 999.01}
                              ,{text_value: 'value9992', long_value: 9990000002, double_value: 999.02}]
      ,val_set  = {            {text_value: 'value9993', long_value: 9990000003, double_value: 999.03}
                              ,{text_value: 'value9994', long_value: 9990000004, double_value: 999.04}}
      ,val_map  = {'map_key1': {text_value: 'value9995', long_value: 9990000005, double_value: 999.05}
                  ,'map_key2': {text_value: 'value9996', long_value: 9990000006, double_value: 999.06}}
 WHERE key='key3';
SELECT * FROM target.udt_mapper;
