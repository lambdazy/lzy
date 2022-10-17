package ai.lzy.model.slot;

import ai.lzy.model.DataScheme;

public interface Slot {
    String STDOUT_SUFFIX = "stdout";
    String STDERR_SUFFIX = "stderr";

    Slot ARGS = new TextLinesInSlot("/dev/args");
    Slot STDIN = new TextLinesInSlot("/dev/stdin");
    Slot STDOUT = new TextLinesOutSlot("/dev/" + Slot.STDOUT_SUFFIX);
    Slot STDERR = new TextLinesOutSlot("/dev/" + Slot.STDERR_SUFFIX);

    String name();

    Media media();

    Direction direction();

    DataScheme contentType();

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
