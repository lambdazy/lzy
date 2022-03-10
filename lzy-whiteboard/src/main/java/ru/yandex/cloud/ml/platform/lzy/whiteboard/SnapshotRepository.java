package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;


public interface SnapshotRepository {

    @NotNull
    SnapshotStatus create(@NotNull Snapshot snapshot) throws IllegalArgumentException;

    @NotNull
    SnapshotStatus createFromSnapshot(@NotNull String fromSnapshotId, @NotNull Snapshot snapshot)
        throws IllegalArgumentException;

    Optional<SnapshotStatus> resolveSnapshot(@NotNull URI id);

    void finalize(@NotNull Snapshot snapshot) throws IllegalArgumentException;

    void error(@NotNull Snapshot snapshot) throws IllegalArgumentException;

    @NotNull
    SnapshotEntry createEntry(@NotNull Snapshot snapshot, @NotNull String id);

    void prepare(@NotNull SnapshotEntry entry, @NotNull String storage, @NotNull List<String> dependentEntryIds)
        throws IllegalArgumentException;

    void commit(@NotNull SnapshotEntry entry, boolean empty) throws IllegalArgumentException;

    Optional<SnapshotEntry> resolveEntry(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotEntryStatus> resolveEntryStatus(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotStatus> lastSnapshot(@NotNull String workflowName, @NotNull String uid);
}
