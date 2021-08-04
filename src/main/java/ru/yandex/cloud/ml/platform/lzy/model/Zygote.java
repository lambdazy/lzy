package ru.yandex.cloud.ml.platform.lzy.model;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataEntity;

import java.util.stream.Stream;

public interface Zygote extends Runnable {
  Slot[] input();
  Slot[] output();

  default Slot slot(String name) {
    return slots()
        .filter(s -> s.name().equals(name))
        .findFirst().orElse(null);
  }

  default Stream<Slot> slots() {
    return Stream.concat(Stream.of(input()), Stream.of(output()));
  }

  @SuppressWarnings("unchecked")
  default Class<DataEntity>[] entities() {return new Class[0];}
}
