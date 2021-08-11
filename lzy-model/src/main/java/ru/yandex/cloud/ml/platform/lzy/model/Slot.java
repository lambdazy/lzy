package ru.yandex.cloud.ml.platform.lzy.model;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public interface Slot {
  String name();
  Media media();
  Direction direction();
  DataSchema contentType();

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
