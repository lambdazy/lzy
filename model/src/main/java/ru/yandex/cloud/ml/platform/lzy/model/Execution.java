package ru.yandex.cloud.ml.platform.lzy.model;

import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface Execution {
    Zygote operation();

    Stream<Communication> incoming();

    Stream<Communication> outgoing();

    ReproducibilityLevel rl();

    enum ReproducibilityLevel {
        ByteLevel,
        StatLevel,
        SratLevel
    }

    interface Communication {
        Slot socket();

        @Nullable
        byte[] content();

        long hash();

        long duration();
    }
}
