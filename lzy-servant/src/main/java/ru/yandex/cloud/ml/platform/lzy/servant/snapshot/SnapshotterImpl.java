package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;

public class SnapshotterImpl implements Snapshotter {
    private final SlotSnapshotProvider snapshotProvider;
    private final Zygote zygote;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final SnapshotMeta meta;
    private final IAM.TaskCredentials taskCred;

    public SnapshotterImpl(IAM.TaskCredentials taskCred, Zygote zygote, SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi, SnapshotMeta meta) {
        this.zygote = zygote;
        this.snapshotApi = snapshotApi;
        this.meta = meta;
        this.taskCred = taskCred;
        snapshotProvider = new SlotSnapshotProvider.Cached(slot -> {
            if (meta.getEntryId(slot.name()) != null) {
                return new S3SlotSnapshot(taskCred.getTaskId(), slot);
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
                zygote.slots()
                        .filter(s -> s.direction().equals(Slot.Direction.INPUT))
                        .filter(s -> meta.getEntryId(s.name()) != null)
                        .forEach(s -> entryBuilder.addDependentEntryIds(meta.getEntryId(s.name()))
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
