package ai.lzy.model.slot;

import ai.lzy.model.DataScheme;

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
    public DataScheme contentType() {
        return DataScheme.PLAIN;
    }
}
