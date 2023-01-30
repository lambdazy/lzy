ALTER TABLE vm ADD COLUMN idle_since TIMESTAMP NULL;
ALTER TABLE vm ADD CONSTRAINT idle_state_check CHECK ((status = 'IDLE' AND idle_since IS NOT NULL) OR
                                                      (status != 'IDLE' AND idle_since IS NULL));
