package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotExecution;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;


public interface SnapshotRepository {

    SnapshotStatus create(Snapshot snapshot);

    SnapshotStatus createFromSnapshot(String fromSnapshotId, Snapshot snapshot);

    @Nullable
    SnapshotStatus resolveSnapshot(URI id);

    void finalize(Snapshot snapshot);

    void error(Snapshot snapshot);

    SnapshotEntryStatus createEntry(Snapshot snapshot, String id);

    void prepare(SnapshotEntry entry, String storage, List<String> dependentEntryIds);

    void commit(SnapshotEntry entry, boolean empty);

    @Nullable
    SnapshotEntry resolveEntry(Snapshot snapshot, String id);

    @Nullable
    SnapshotEntryStatus resolveEntryStatus(Snapshot snapshot, String id);

    @Nullable
    SnapshotStatus lastSnapshot(String workflowName, String uid);

    List<SnapshotExecution> findExecutions(String name, String snapshot);

    void saveExecution(SnapshotExecution execution);
}
