package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public interface LzyServerTestContext extends AutoCloseable {
    String address(boolean fromDocker);
    TaskType type();

    LzyServerGrpc.LzyServerBlockingStub client();

    void init();
    void close();

    enum TaskType {
        PROCESS("local-process"),
        DOCKER("local-docker");

        private final String typeStr;

        TaskType(String s) {
            typeStr = s;
        }

        public String toString() {
            return typeStr;
        }
    }
}
