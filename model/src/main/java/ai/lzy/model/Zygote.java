package ai.lzy.model;

import ai.lzy.model.data.DataEntity;
import java.util.stream.Stream;

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
