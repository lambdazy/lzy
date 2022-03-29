package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;

public interface Snapshooter extends AutoCloseable {
    void registerSlot(LzySlot slot, String snapshotId, String entryId);
    void close() throws Exception;
}
