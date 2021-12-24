package ru.yandex.cloud.ml.platform.lzy.test;

import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public interface LzyServerTestContext extends AutoCloseable {
    String address(boolean fromDocker);
    TasksConfig.TaskType type();

    LzyServerGrpc.LzyServerBlockingStub client();

    void init(boolean fromDocker);
    void close();
}
