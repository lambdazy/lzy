package ru.yandex.cloud.ml.platform.lzy.model.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

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
