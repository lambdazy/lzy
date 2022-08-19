ALTER TABLE whiteboard
    ADD COLUMN namespace text;

CREATE TABLE whiteboard_tag (
    wb_id  text  REFERENCES whiteboard (wb_id) ON UPDATE CASCADE ON DELETE CASCADE,
    tag    text,
    PRIMARY KEY (wb_id, tag)
);