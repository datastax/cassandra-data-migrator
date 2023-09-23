DROP TABLE IF EXISTS origin.smoke_vector;
CREATE TABLE origin.smoke_vector (col1 int PRIMARY KEY, embedding vector<float, 3>);
INSERT INTO origin.smoke_vector(col1, embedding) VALUES (1,[1.1,2.2,3.3]);
INSERT INTO origin.smoke_vector(col1, embedding) VALUES (2,[4.4,-0.01,0.99]);

DROP TABLE IF EXISTS target.smoke_vector;
CREATE TABLE target.smoke_vector (col1 int PRIMARY KEY, embedding vector<float, 3>);