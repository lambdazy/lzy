package ai.lzy.whiteboard;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.snapshot.ExecutionSnapshot;
import ai.lzy.model.snapshot.Snapshot;
import ai.lzy.model.snapshot.SnapshotEntry;
import ai.lzy.model.snapshot.SnapshotEntryStatus;
import ai.lzy.model.snapshot.SnapshotStatus;
import ai.lzy.whiteboard.exceptions.SnapshotRepositoryException;

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

    void prepare(@NotNull SnapshotEntry entry, @NotNull String storage, @NotNull List<String> dependentEntryIds,
        @NotNull DataSchema schema) throws SnapshotRepositoryException;

    void commit(@NotNull SnapshotEntry entry, boolean empty) throws SnapshotRepositoryException;

    void abort(@NotNull SnapshotEntry entry) throws SnapshotRepositoryException;

    Optional<SnapshotEntry> resolveEntry(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotEntryStatus> resolveEntryStatus(@NotNull Snapshot snapshot, @NotNull String id);

    Optional<SnapshotStatus> lastSnapshot(@NotNull String workflowName, @NotNull String uid);

    Stream<ExecutionSnapshot> executionSnapshots(@NotNull String name, @NotNull String snapshot);

    void saveExecution(@NotNull ExecutionSnapshot execution);
}
