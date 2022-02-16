package ru.yandex.cloud.ml.platform.lzy.model.slots;


import java.nio.file.Path;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface FileSlot extends Slot {
    Path mount();

    @Override
    default Media media() {
        return Media.FILE;
    }
}
