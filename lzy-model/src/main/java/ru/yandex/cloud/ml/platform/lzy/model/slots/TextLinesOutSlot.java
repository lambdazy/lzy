package ru.yandex.cloud.ml.platform.lzy.model.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.PlainTextFileSchema;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;

public class TextLinesOutSlot implements Slot {
    private final AtomicZygote zygote;
    private final String name;

    public TextLinesOutSlot(AtomicZygote zygote, String name) {
        this.zygote = zygote;
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
        return new PlainTextFileSchema();
    }
}
