package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Context.SlotAssignment;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage.SnapshotStorage;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class SnapshotterImpl implements Snapshotter {
    private final SlotSnapshotProvider snapshotProvider;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.TaskCredentials taskCred;
    private final List<SlotAssignment> assignments;
    private final SnapshotMeta meta;
    private final Logger logger = LogManager.getLogger(SnapshotterImpl.class);

    public SnapshotterImpl(IAM.TaskCredentials taskCred, String bucket,
                           SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
                           SnapshotStorage storage, Stream<SlotAssignment> assignments, SnapshotMeta meta) {
        this.meta = meta;
        this.snapshotApi = snapshotApi;
        this.taskCred = taskCred;
        this.assignments = assignments.collect(Collectors.toList());
        snapshotProvider = new SlotSnapshotProvider.Cached(slot -> {
            logger.info(String.format("Creating new SlotSnapshotter for slot %s with entry %s", slot.name(),
                meta.getEntryId(slot.name())));
            if (meta.getEntryId(slot.name()) != null) {
                return new S3SlotSnapshot(taskCred.getTaskId(), bucket, slot, storage);
            } else {
                return new DevNullSlotSnapshot(slot);
            }
        });
    }

    @Override
    public void prepare(Slot slot) {
        if (meta.getEntryId(slot.name()) != null) {
            final URI uri = snapshotProvider().slotSnapshot(slot).uri();
            final LzyWhiteboard.SnapshotEntry.Builder entryBuilder = LzyWhiteboard.SnapshotEntry.newBuilder()
                .setEntryId(meta.getEntryId(slot.name()))
                .setStorageUri(uri.toString());
            if (slot.direction().equals(Slot.Direction.OUTPUT)) {
                assignments.stream()
                    .filter(s -> s.slot().direction().equals(Slot.Direction.INPUT))
                    .filter(s -> meta.getEntryId(s.slot().name()) != null)
                    .forEach(s -> entryBuilder.addDependentEntryIds(meta.getEntryId(s.slot().name()))
                    );
            }

            LzyWhiteboard.PrepareCommand command = LzyWhiteboard.PrepareCommand
                .newBuilder()
                .setSnapshotId(meta.getSnapshotId())
                .setEntry(entryBuilder.build())
                .setAuth(IAM.Auth.newBuilder().setTask(taskCred).build())
                .build();
            LzyWhiteboard.OperationStatus status = snapshotApi.prepareToSave(command);
            if (status.getStatus().equals(LzyWhiteboard.OperationStatus.Status.FAILED)) {
                throw new RuntimeException("LzyExecution::configureSlot failed to save to snapshot");
            }
        }
    }

    @Override
    public void commit(Slot slot) {
        if (meta.getEntryId(slot.name()) != null) {
            LzyWhiteboard.CommitCommand commitCommand = LzyWhiteboard.CommitCommand
                .newBuilder()
                .setSnapshotId(meta.getSnapshotId())
                .setEntryId(meta.getEntryId(slot.name()))
                .setEmpty(snapshotProvider().slotSnapshot(slot).isEmpty())
                .setAuth(IAM.Auth.newBuilder().setTask(taskCred).build())
                .build();
            LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
            if (status.getStatus().equals(LzyWhiteboard.OperationStatus.Status.FAILED)) {
                throw new RuntimeException("LzyExecution::configureSlot failed to commit to whiteboard");
            }
        }
    }

    @Override
    public SlotSnapshotProvider snapshotProvider() {
        return snapshotProvider;
    }
}
