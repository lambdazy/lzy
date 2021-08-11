package ru.yandex.cloud.ml.platform.lzy.model.graph;

import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface Graph extends Zygote {
  Slot[] allSockets();

  interface Builder {
    Builder append(Zygote op);
    Builder link(Slot from, Slot to, Channel ch);

    Graph build();
  }
}
