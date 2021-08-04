package ru.yandex.cloud.ml.platform.lzy.model.data;

import java.util.stream.Stream;

public interface DataStream extends Stream<DataPage> {
  DataSnapshot snapshot();

  Comparable version();
}
