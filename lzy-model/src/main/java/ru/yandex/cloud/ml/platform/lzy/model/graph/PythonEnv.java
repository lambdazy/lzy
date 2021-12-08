package ru.yandex.cloud.ml.platform.lzy.model.graph;

import java.util.List;

public interface PythonEnv extends Env { // extends #EnvConfig
    String name();

    String yaml();

    List<LocalModule> localModules();
}
