package ru.yandex.cloud.ml.platform.lzy.model.graph;

import java.util.Map;

public interface PythonEnv extends Env {
    String name();
    String yaml();
    Map<String, String> localModules();
}
