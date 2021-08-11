package ru.yandex.cloud.ml.platform.lzy.model.slots;


import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import java.nio.file.Path;

public interface FileSlot extends Slot {
  Path mount();

  @Override
  default Media media() {
    return Media.FILE;
  }
}
