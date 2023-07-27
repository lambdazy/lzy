CREATE TYPE image_kind AS ENUM ('WORKER', 'SYNC', 'JUPYTERLAB');

CREATE TABLE images
(
    kind              image_kind NOT NULL,
    image             TEXT       NOT NULL,
    additional_images TEXT[],
    created_at        TIMESTAMP  NOT NULL DEFAULT NOW(),

    CONSTRAINT additional_images_check
        CHECK ((kind = 'JUPYTERLAB' AND additional_images IS NOT NULL) OR
               (kind != 'JUPYTERLAB' AND additional_images IS NULL))
);

CREATE UNIQUE INDEX single_sync_image_idx ON images (kind) WHERE kind = 'SYNC'::image_kind;
