package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

public interface SnapshotEntryStatus {
    boolean empty();
    State status();
    SnapshotEntry entry();

    // IN_PROGRESS --> started saving data
    // FINISHED --> finished saving data
    enum State {
        IN_PROGRESS,
        FINISHED
    }
}
