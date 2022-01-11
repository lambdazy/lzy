CREATE TABLE snapshot_owner (
    snapshot_id  text  PRIMARY KEY REFERENCES snapshot (snapshot_id) ON UPDATE CASCADE ON DELETE CASCADE,
    uid          text  NOT NULL
);