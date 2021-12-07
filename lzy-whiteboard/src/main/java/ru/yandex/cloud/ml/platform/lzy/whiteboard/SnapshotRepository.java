package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.stream.Stream;

public interface SnapshotRepository {
    void create(Snapshot snapshot);
    @Nullable
    SnapshotStatus resolveSnapshot(URI id);
    void finalize(Snapshot snapshot);
    void error(Snapshot snapshot);

    void prepare(SnapshotEntry entry);
    @Nullable
    SnapshotEntry resolveEntry(Snapshot snapshot, String id);
    void commit(SnapshotEntry entry);
    // Stream<SnapshotEntry> entries(Snapshot snapshot);
}
