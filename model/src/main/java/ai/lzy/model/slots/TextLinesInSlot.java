package ai.lzy.model.slots;

import ai.lzy.model.Slot;
import ai.lzy.model.data.DataSchema;

public class TextLinesInSlot implements Slot {
    private final String name;

    public TextLinesInSlot(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Media media() {
        return Media.PIPE;
    }

    @Override
    public Direction direction() {
        return Direction.INPUT;
    }

    @Override
    public DataSchema contentType() {
        return DataSchema.plain;
    }
}
