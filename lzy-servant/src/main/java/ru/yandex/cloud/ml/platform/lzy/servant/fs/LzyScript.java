package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.nio.file.Path;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

public interface LzyScript {
    Zygote operation();

    Path location();

    CharSequence scriptText();
}
