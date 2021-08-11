package ru.yandex.cloud.ml.platform.lzy.model.data;


import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Execution;

import java.net.URI;

public interface DataEntity extends DataPage {
  Component[] components();

  interface Component {
    URI id();
    Slot source();
    Necessity necessity();

    enum Necessity {
      Needed,
      Supplementary,
      GoodToKnow,
      Temp
    }
  }
}
