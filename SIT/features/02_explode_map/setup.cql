DROP TABLE IF EXISTS source.feature_explode_map;
CREATE TABLE source.feature_explode_map(key text, value text, fruits map<text,int>, PRIMARY KEY (key));
INSERT INTO source.feature_explode_map(key,value,fruits) VALUES ('key1','valueA', {'apples': 3, 'oranges': 5, 'bananas': 2, 'grapes': 11});
INSERT INTO source.feature_explode_map(key,value,fruits) VALUES ('key2','valueB', {'apples': 4, 'oranges': 6, 'bananas': 3, 'pears': 7});
INSERT INTO source.feature_explode_map(key,value,fruits) VALUES ('key3','valueC', {'apples': 5, 'oranges': 7, 'bananas': 4, 'kiwi': 42});

DROP TABLE IF EXISTS target.feature_explode_map;
CREATE TABLE target.feature_explode_map(key text, fruit text, value text, fruit_qty int, PRIMARY KEY ((key), fruit));
