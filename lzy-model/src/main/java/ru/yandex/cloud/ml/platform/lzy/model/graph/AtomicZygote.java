package ru.yandex.cloud.ml.platform.lzy.model.graph;


import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

public interface AtomicZygote extends Zygote {
  Env env();
  Provisioning provisioning();
  String fuze();
}
