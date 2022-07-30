package ru.yandex.cloud.ml.platform.model.util.latch;

import javax.annotation.Nullable;

public interface LatchManager {

    LatchManager withPrefix(String prefix);

    Latch create(String key, int count);

    @Nullable
    Latch get(String key);

    void remove(String key);

}
