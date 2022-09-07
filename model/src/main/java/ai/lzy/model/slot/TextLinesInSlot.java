package ai.lzy.model.slot;

import ai.lzy.model.data.DataSchema;

public record TextLinesInSlot(String name) implements Slot {

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
