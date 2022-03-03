package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface Snapshotter {
    void prepare(Slot slot, String snapshotId, String entryId);

    void commit(Slot slot, String snapshotId, String entryId);

    SlotSnapshotProvider snapshotProvider();

    String storageUrlForEntry(String snapshotId, String entryId);

    class DevNullSnapshotter implements Snapshotter {
        private final SlotSnapshotProvider snapshotProvider = new SlotSnapshotProvider.Cached(DevNullSlotSnapshot::new);

        @Override
        public void prepare(Slot slot, String snapshotId, String entryId) {

        }

        @Override
        public void commit(Slot slot, String snapshotId, String entryId) {

        }

        @Override
        public SlotSnapshotProvider snapshotProvider() {
            return snapshotProvider;
        }

        @Override
        public String storageUrlForEntry(String snapshotId, String entryId) {
            return null;
        }
    }
}
