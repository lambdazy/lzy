package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.ExecutionSnapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.SnapshotRepositoryException;


public interface SnapshotRepository {

    @NotNull
    SnapshotStatus create(@NotNull Snapshot snapshot) throws SnapshotRepositoryException;

    @NotNull
    SnapshotStatus createFromSnapshot(@NotNull String fromSnapshotId, @NotNull Snapshot snapshot)
        throws SnapshotRepositoryException;

    Optional<SnapshotStatus> resolveSnapshot(@NotNull URI id);

    void finalize(@NotNull Snapshot snapshot) throws SnapshotRepositoryException;

    void error(@NotNull Snapshot snapshot) throws SnapshotRepositoryException;

    @NotNull
    SnapshotEntry createEntry(@NotNull Snapshot snapshot, @NotNull String id) throws SnapshotRepositoryException;

    void prepare(@NotNull SnapshotEntry entry, @NotNull String storage, @NotNull List<String> dependentEntryIds)
        throws SnapshotRepositoryException;

    void commit(@NotNull SnapshotEntry entry, boolean empty) throws SnapshotRepositoryException;

    void abort(@NotNull SnapshotEntry entry) throws SnapshotRepositoryException;

    Optional<SnapshotEntry> resolveEntry(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotEntryStatus> resolveEntryStatus(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotStatus> lastSnapshot(@NotNull String workflowName, @NotNull String uid);

    Stream<ExecutionSnapshot> executionSnapshots(@NotNull String name, @NotNull String snapshot);

    void saveExecution(@NotNull ExecutionSnapshot execution);
}
