package ai.lzy.model;

import ai.lzy.model.data.DataSchema;
import ai.lzy.model.slots.TextLinesInSlot;
import ai.lzy.model.slots.TextLinesOutSlot;

public interface Slot {
    Slot ARGS = new TextLinesInSlot("/dev/args");
    Slot STDIN = new TextLinesInSlot("/dev/stdin");
    Slot STDOUT = new TextLinesOutSlot("/dev/stdout");
    Slot STDERR = new TextLinesOutSlot("/dev/stderr");

    String name();

    Media media();

    Direction direction();

    DataSchema contentType();

    enum Direction {
        INPUT,
        OUTPUT
    }

    enum Media {
        FILE(java.nio.file.Path.class),
        PIPE(java.nio.file.Path.class),
        ARG(java.lang.String.class);

        private final Class type;

        Media(Class type) {
            this.type = type;
        }

        public Class of() {
            return type;
        }
    }
}
