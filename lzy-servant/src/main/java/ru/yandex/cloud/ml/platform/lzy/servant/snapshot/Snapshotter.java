package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface Snapshotter {
    void prepare(Slot slot);
    void commit(Slot slot);
    ExecutionSnapshot snapshot();

    class DevNullSnapshotter implements Snapshotter {
        private final ExecutionSnapshot snapshot = new DevNullExecutionSnapshot();

        @Override
        public void prepare(Slot slot) {

        }

        @Override
        public void commit(Slot slot) {

        }

        @Override
        public ExecutionSnapshot snapshot() {
            return snapshot;
        }
    }
}
