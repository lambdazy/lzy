package ru.yandex.cloud.ml.platform.lzy.model.graph;


import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public interface AtomicZygote extends Zygote {
  Env env();
  String description();
  String fuze();
  Provisioning provisioning();
  Operations.Zygote zygote();
}
