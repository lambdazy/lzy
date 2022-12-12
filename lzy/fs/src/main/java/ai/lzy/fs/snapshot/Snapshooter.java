package ai.lzy.fs.snapshot;

import ai.lzy.fs.fs.LzySlot;

public interface Snapshooter extends AutoCloseable {
    void registerSlot(LzySlot slot, String snapshotId, String entryId);
    void close() throws InterruptedException;
}
