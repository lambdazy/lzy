package ru.yandex.cloud.ml.platform.lzy.model.graph;


import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

public interface Mutant extends Zygote {
  Zygote inception();
}
