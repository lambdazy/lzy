package ru.yandex.cloud.ml.platform.lzy.model.graph;


import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.util.Properties;

public interface AtomicZygote extends Zygote {
  Provisioning provisioning();
  Container container();

  String fuze();
}
