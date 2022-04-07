package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public interface Snapshooter extends AutoCloseable {
    void registerSlot(LzySlot slot, String snapshotId, String entryId);
    void close() throws Exception;

    StorageClient storage();
}
