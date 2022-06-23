package ai.lzy.model.slots;

import ai.lzy.model.Slot;
import ai.lzy.model.data.DataSchema;

public class TextLinesOutSlot implements Slot {
    private final String name;

    public TextLinesOutSlot(String name) {
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
        return Direction.OUTPUT;
    }

    @Override
    public DataSchema contentType() {
        return DataSchema.plain;
    }
}
