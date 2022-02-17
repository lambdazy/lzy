package ru.yandex.cloud.ml.platform.lzy.model;

import java.net.URI;
import java.util.UUID;
import javax.annotation.Nullable;

public interface SlotStatus {
    @Nullable
    String channelId();

    String user();

    UUID tid();

    Slot slot();

    @Nullable
    URI connected();

    long pointer();

    State state();

    enum State {
        PREPARING, UNBOUND, OPEN, CLOSED, SUSPENDED
    }
}
