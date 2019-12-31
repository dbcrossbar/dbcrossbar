CREATE TABLE legacy_json (
    id int NOT NULL PRIMARY KEY,
    data json -- Not jsonb!
);
