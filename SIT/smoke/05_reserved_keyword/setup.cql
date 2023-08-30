DROP TABLE IF EXISTS origin.smoke_order;
CREATE TABLE origin.smoke_order (col1 text primary key, "order" text);
INSERT INTO origin.smoke_order(col1, "order") VALUES ('c1','o1');
INSERT INTO origin.smoke_order(col1, "order") VALUES ('c2','o2');

DROP TABLE IF EXISTS target.smoke_order;
CREATE TABLE target.smoke_order (col1 text primary key, "order" text);


