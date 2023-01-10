CREATE TABLE job
(
    id               TEXT      NOT NULL PRIMARY KEY,
    provider_class   TEXT      NOT NULL,
    status           TEXT      NOT NULL,
    serialized_input TEXT      NULL,
    start_after      TIMESTAMP NULL
);

CREATE INDEX active_jobs_index ON job (id) WHERE status != 'DONE';