package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;

public class SnapshotterImpl implements Snapshotter {
    private final Zygote zygote;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final SnapshotMeta meta;
    private final ExecutionSnapshot executionSnapshot;

    public SnapshotterImpl(String taskId, Zygote zygote, SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi, SnapshotMeta meta) {
        this.zygote = zygote;
        this.snapshotApi = snapshotApi;
        this.meta = meta;
        executionSnapshot = new S3ExecutionSnapshot(taskId);
    }

    @Override
    public void prepare(Slot slot) {
        if (meta.getEntryId(slot.name()) != null) {
            final URI uri = executionSnapshot.getSlotUri(slot);
            LzyWhiteboard.PrepareCommand.Builder builder = LzyWhiteboard.PrepareCommand
                    .newBuilder()
                    .setSnapshotId(meta.getSnapshotId())
                    .setEntryId(meta.getEntryId(slot.name()))
                    .setUri(uri.toString());
            if (slot.direction().equals(Slot.Direction.OUTPUT)) {
                zygote.slots()
                        .filter(s -> s.direction().equals(Slot.Direction.INPUT))
                        .forEach(s -> builder.setDependency(LzyWhiteboard.Dependency
                                .newBuilder()
                                .addDepEntryId(meta.getEntryId(s.name()))
                                .build())
                        );
            }
            LzyWhiteboard.OperationStatus status = snapshotApi.prepareToSave(builder.build());
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
                    .setEmpty(executionSnapshot.isEmpty(slot))
                    .build();
            LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
            if (status.getStatus().equals(LzyWhiteboard.OperationStatus.Status.FAILED)) {
                throw new RuntimeException("LzyExecution::configureSlot failed to commit to whiteboard");
            }
        }
    }

    @Override
    public ExecutionSnapshot snapshot() {
        return executionSnapshot;
    }
}
