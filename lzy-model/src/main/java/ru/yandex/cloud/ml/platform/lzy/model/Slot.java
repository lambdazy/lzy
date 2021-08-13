package ru.yandex.cloud.ml.platform.lzy.model;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;

public interface Slot {
  String name();
  Media media();
  Direction direction();
  DataSchema contentType();

  Slot ARGS = new TextLinesInSlot("/dev/args");
  Slot STDIN = new TextLinesInSlot("/dev/stdin");
  Slot STDOUT = new TextLinesOutSlot("/dev/stdout");
  Slot STDERR = new TextLinesOutSlot("/dev/stderr");

  enum Direction {
    INPUT,
    OUTPUT
  }

  enum Media {
    FILE(java.nio.file.Path.class),
    PIPE(java.nio.file.Path.class),
    ARG(java.lang.String.class);

    private final Class type;

    public Class of() {
      return type;
    }
    Media(Class type) {
      this.type = type;
    }
  }
}
