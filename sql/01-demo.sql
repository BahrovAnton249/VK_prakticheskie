SHOW CATALOGS;

CREATE SCHEMA lakekeeper.lakehouse;

CREATE TABLE lakekeeper.lakehouse.people (
    id bigint,
    name varchar
);

INSERT INTO lakekeeper.lakehouse.people VALUES
    (1, 'alice'),
    (2, 'bob');

SELECT * FROM lakekeeper.lakehouse.people ORDER BY id;
