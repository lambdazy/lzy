package ru.yandex.cloud.ml.platform.lzy.model.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

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
