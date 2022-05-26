package ru.yandex.cloud.ml.platform.lzy.model;

import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataEntity;

public interface Zygote extends Runnable {
    String name();

    Slot[] input();

    Slot[] output();

    default Slot slot(String name) {
        return slots()
            .filter(s -> s.name().equals(name))
            .findFirst().orElse(null);
    }

    default Stream<Slot> slots() {
        return Stream.of(
            Stream.of(Slot.STDIN, Slot.STDOUT, Slot.STDERR),
            Stream.of(input()),
            Stream.of(output())
        ).flatMap(s -> s);
    }

    @SuppressWarnings("unchecked")
    default Class<DataEntity>[] entities() {
        return new Class[0];
    }
}
