DROP TABLE IF EXISTS 'tweets';
CREATE TABLE 'tweets' (
    id BIGINT,
    date DATETIME,
    json VARCHAR(10000),
    PRIMARY KEY('id')
);
