create type image_kind as ENUM ('CACHE', 'SYNC');

CREATE TABLE images
(
    kind              image_kind NOT NULL,
    images            TEXT[],
    sync_image        TEXT,
    additional_images TEXT[],
    pool_kind         TEXT       NOT NULL,
    pool_name         TEXT       NOT NULL,
    created_at        TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX single_sync_image_idx ON images (kind) WHERE kind = 'SYNC'::image_kind;
