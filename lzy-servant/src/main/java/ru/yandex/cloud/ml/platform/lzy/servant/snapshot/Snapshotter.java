package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface Snapshotter {
    void prepare(Slot slot);
    void commit(Slot slot);
    SlotSnapshotProvider snapshotProvider();

    class DevNullSnapshotter implements Snapshotter {
        private final SlotSnapshotProvider snapshotProvider = new SlotSnapshotProvider.Cached(DevNullSlotSnapshot::new);

        @Override
        public void prepare(Slot slot) {

        }

        @Override
        public void commit(Slot slot) {

        }

        @Override
        public SlotSnapshotProvider snapshotProvider() {
            return snapshotProvider;
        }
    }
}
