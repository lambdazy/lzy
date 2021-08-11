package ru.yandex.cloud.ml.platform.lzy.model;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public interface Execution {
  Zygote operation();

  Stream<Communication> incoming();
  Stream<Communication> outgoing();
  ReproducibilityLevel rl();

  interface Communication {
    Slot socket();
    @Nullable
    byte[] content();
    long hash();
    long duration();
  }

  enum ReproducibilityLevel {
    ByteLevel,
    StatLevel,
    SratLevel
  }
}
