package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.nio.file.Path;

public interface LzyScript {
    Zygote operation();
    Path location();

    CharSequence scriptText();
}
