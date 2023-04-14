ALTER TABLE vm
    DROP COLUMN vm_subject_id;

CREATE UNIQUE INDEX vm_ott_index ON vm (vm_ott) WHERE vm_ott != '';

