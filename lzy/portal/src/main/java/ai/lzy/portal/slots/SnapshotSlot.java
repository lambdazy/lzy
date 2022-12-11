package ai.lzy.portal.slots;

public interface SnapshotSlot {
    SnapshotSlotStatus snapshotState();

    enum SnapshotSlotStatus {
        INITIALIZING,
        SYNCING,
        SYNCED,
        FAILED
    }
}
