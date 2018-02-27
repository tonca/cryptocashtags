DROP TABLE IF EXISTS 'tweets';
CREATE TABLE 'tweets' (
    id BIGINT,
    date DATETIME,
    json JSON1,
    filter VARCHAR(10),
    PRIMARY KEY('id')
);