package ru.yandex.cloud.ml.platform.lzy.snapshot;

import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.storage.StorageClient;

public interface Snapshooter extends AutoCloseable {
    void registerSlot(LzySlot slot, String snapshotId, String entryId);
    void close() throws InterruptedException;
}
