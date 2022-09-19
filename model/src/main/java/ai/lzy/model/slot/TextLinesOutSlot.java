package ai.lzy.model.slot;

import ai.lzy.model.DataScheme;

public record TextLinesOutSlot(String name) implements Slot {

    @Override
    public Media media() {
        return Media.PIPE;
    }

    @Override
    public Direction direction() {
        return Direction.OUTPUT;
    }

    @Override
    public DataScheme contentType() {
        return DataScheme.PLAIN;
    }
}
