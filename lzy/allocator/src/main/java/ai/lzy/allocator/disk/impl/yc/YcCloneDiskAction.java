package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.compute.v1.SnapshotServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class YcCloneDiskAction extends YcDiskActionBase<YcCloneDiskState> {

    public YcCloneDiskAction(DiskManager.OuterOperation op, YcCloneDiskState state, YcDiskManager diskManager) {
        super(op, "[YcCloneDisk]", state, diskManager);

        log().info("Clone disk {}; clone name={} size={}Gb zone={}, outerOp={}",
            state.originDisk().spec().name(), state.newDiskSpec().name(), state.newDiskSpec().sizeGb(),
            state.newDiskSpec().zone(), opId());
    }

    @Override
    protected void notifyExpired() {
        metrics().cloneDiskError.inc();
        metrics().cloneDiskTimeout.inc();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        super.onExpired(tx);
        // TODO: remove snapshot on error
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::startCreateSnapshot, this::waitSnapshot, this::startCreateDisk, this::waitDisk,
            // TODO: emit new operation
            this::startDeleteSnapshot, this::waitCleanup);
    }

    private StepResult startCreateSnapshot() {
        if (!state.ycCreateSnapshotOperationId().isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        String ycOpId;
        try {
            var ycOp = ycSnapshotService()
                .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId() + "-CreateSnapshot"))
                .create(
                    SnapshotServiceOuterClass.CreateSnapshotRequest.newBuilder()
                        .setFolderId(state.folderId())
                        .setDiskId(state.originDisk().id())
                        .setDescription("CloneDisk: '%s', operation: '%s'"
                            .formatted(state.originDisk().id(), opId()))
                        .build());
            ycOpId = ycOp.getId();
        } catch (StatusRuntimeException e) {
            log().error("Error while creating YcCloneDisk::CreateSnapshot op {} state: [{}] {}",
                opId(), e.getStatus().getCode(), e.getStatus().getDescription());

            // TODO: handle ALREADY_EXIST

            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        failOperation(e.getStatus(), tx);
                        diskOpDao().deleteDiskOp(opId(), tx);
                        tx.commit();
                    }
                });

                metrics().cloneDiskError.inc();
            } catch (Exception ex) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Error while failing YcCloneDisk::CreateSnapshot op {}: {}. Reschedule...",
                    opId(), e.getMessage());
                return StepResult.RESTART;
            }

            return StepResult.FINISH;
        }

        InjectedFailures.failCloneDisk1();

        return saveState(
            () -> {
                state = state.withCreateSnapshotOperationId(ycOpId);
                log().info("Wait YC at YcCloneDisk::CreateSnapshot {}/{}...",
                    opId(), state.ycCreateSnapshotOperationId());
            },
            () -> metrics().cloneDiskRetryableError.inc()
        );
    }

    private StepResult waitSnapshot() {
        if (state.snapshotId() != null) {
            return StepResult.ALREADY_DONE;
        }

        log().info("Test status of YcCloneDisk::CreateSnapshot operation {}/{}",
            opId(), state.ycCreateSnapshotOperationId());

        final OperationOuterClass.Operation ycGetSnapshotOp;
        try {
            ycGetSnapshotOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycCreateSnapshotOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().cloneDiskRetryableError.inc();
            log().error("Error while getting YcCloneDisk::CreateSnapshot op {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycCreateSnapshotOperationId(), e.getStatus().getCode(),
                e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycGetSnapshotOp.getDone()) {
            log().info("YcCloneDisk::CreateSnapshot {}/{} not completed yet, reschedule...",
                opId(), state.ycCreateSnapshotOperationId());
            return StepResult.RESTART;
        }

        if (ycGetSnapshotOp.hasError()) {
            log().warn("YcCloneDisk::CreateSnapshot operation {}/{} failed with error {}",
                opId(), state.ycCreateSnapshotOperationId(), ycGetSnapshotOp.getError());
            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        failOperation(
                            Status.fromCodeValue(ycGetSnapshotOp.getError().getCode())
                                .withDescription(ycGetSnapshotOp.getError().getMessage()),
                            tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Cannot complete failed YcCloneDisk::CreateSnapshot operation {}/{}: {}. Reschedule...",
                    opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                return StepResult.RESTART;
            }

            // don't restart
            return StepResult.FINISH;
        }

        assert ycGetSnapshotOp.hasResponse();

        log().warn("YcCloneDisk::CreateSnapshot operation {}/{} successfully completed",
            opId(), state.ycCreateSnapshotOperationId());

        String snapshotId;
        try {
            snapshotId = ycGetSnapshotOp.getMetadata()
                .unpack(SnapshotServiceOuterClass.CreateSnapshotMetadata.class).getSnapshotId();
        } catch (InvalidProtocolBufferException e) {
            log().error("Cannot parse CreateSnapshotMetadata, YcCloneDisk::CreateSnapshot {}/{} failed",
                opId(), state.ycCreateSnapshotOperationId(), e);
            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().failDiskOp(opId(), e.getMessage(), tx);
                        failOperation(Status.INTERNAL.withDescription(e.getMessage()), tx);
                        tx.commit();
                    }
                });
            } catch (Exception ex) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                    opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                return StepResult.RESTART;
            }

            metrics().cloneDiskError.inc();
            // don't restart
            return StepResult.FINISH;
        }

        return saveState(
            () -> state = state.withSnapshotId(snapshotId),
            () -> metrics().cloneDiskRetryableError.inc()
        );
    }

    private StepResult startCreateDisk() {
        if (!state.ycCreateDiskOperationId().isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        assert state.snapshotId() != null;

        var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
            .setName(state.newDiskSpec().name())
            .setDescription("Cloned disk '%s'".formatted(state.originDisk().id()))
            .setFolderId(state.folderId())
            .setSize(((long) state.newDiskSpec().sizeGb()) << YcDiskManager.GB_SHIFT)
            .setTypeId(state.newDiskSpec().type().toYcName())
            .setZoneId(state.newDiskSpec().zone())
            .putLabels(YcDiskManager.USER_ID_LABEL, state.newDiskMeta().user())
            .setSnapshotId(state.snapshotId());

        String ycCreateDiskOperationId;
        try {
            var ycCreateDiskOperation = ycDiskService()
                .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId() + "-CreateDisk"))
                .create(diskRequestBuilder.build());

            ycCreateDiskOperationId = ycCreateDiskOperation.getId();
        } catch (StatusRuntimeException e) {
            log().error("Error while running YcCloneDisk::CreateDisk op {} state: [{}] {}. Reschedule...",
                opId(), e.getStatus().getCode(), e.getStatus().getDescription());

            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        failOperation(e.getStatus(), tx);
                        diskOpDao().deleteDiskOp(opId(), tx);
                        tx.commit();
                    }
                });

                metrics().cloneDiskError.inc();
            } catch (Exception ex) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Error while failing YcCloneDisk::CreateDisk op {}: {}. Reschedule...",
                    opId(), e.getMessage());
                return StepResult.RESTART;
            }

            return StepResult.FINISH;
        }

        InjectedFailures.failCloneDisk2();

        return saveState(
            () -> {
                state = state.withCreateDiskOperationId(ycCreateDiskOperationId);
                log().info("Wait YC at YcCloneDisk::CreateDisk {}/{}...", opId(), state.ycCreateDiskOperationId());
            },
            () -> metrics().cloneDiskRetryableError.inc()
        );
    }

    private StepResult waitDisk() {
        if (state.newDiskId() != null && !state.newDiskId().isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        log().info("Test status of YcCloneDisk::CreateDisk operation {}/{}", opId(), state.ycCreateDiskOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycCreateDiskOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().cloneDiskRetryableError.inc();
            log().error("Error while getting YcCloneDisk::CreateDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycCreateDiskOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            log().debug("YcCloneDisk::CreateDisk {}/{} not completed yet, reschedule...",
                opId(), state.ycCreateDiskOperationId());
            return StepResult.RESTART;
        }

        if (ycOp.hasResponse()) {
            final Disk newDisk;
            try {
                var diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
                newDisk = new Disk(diskId, state.newDiskSpec(), state.newDiskMeta());

                log().info("YcCloneDisk::CreateDisk op {}/{} succeeded, created disk {}",
                    opId(), state.ycCreateDiskOperationId(), newDisk);

            } catch (InvalidProtocolBufferException e) {
                log().error("Cannot parse CreateDiskMetadata, YcCloneDisk::CreateDisk {}/{} failed",
                    opId(), state.ycCreateDiskOperationId(), e);
                try {
                    withRetries(log(), () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            diskOpDao().failDiskOp(opId(), e.getMessage(), tx);
                            failOperation(Status.INTERNAL.withDescription(e.getMessage()), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    log().error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    return StepResult.RESTART;
                }

                metrics().cloneDiskError.inc();
                // don't restart
                return StepResult.FINISH;
            }

            var meta = Any.pack(
                DiskServiceApi.CloneDiskMetadata.newBuilder()
                    .setDiskId(newDisk.id())
                    .build());

            var resp = Any.pack(
                DiskServiceApi.CloneDiskResponse.newBuilder()
                    .setDisk(newDisk.toProto())
                    .build());

            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().updateDiskOp(opId(), toJson(state), tx);
                        diskDao().insert(newDisk, tx);
                        completeOperation(meta, resp, tx);
                        tx.commit();
                    }
                });

                state = state.withNewDiskId(newDisk.id());
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Cannot complete successful YcCloneDisk::CreateDisk operation {}/{}: {}. Reschedule...",
                    opId(), state.ycCreateDiskOperationId(), e.getMessage());
                return StepResult.RESTART;
            }

            metrics().cloneDiskFinish.inc();
            metrics().cloneDiskDuration.observe(Duration.between(op.startedAt(), Instant.now()).getSeconds());

            // don't restart
            return StepResult.FINISH;
        }

        // CreateDisk failed

        try {
            log().warn("YcCloneDisk::CreateDisk op {}/{} failed with error {}",
                opId(), state.ycCreateDiskOperationId(), ycOp.getError());

            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    diskOpDao().deleteDiskOp(opId(), tx);
                    failOperation(
                        Status.fromCodeValue(ycOp.getError().getCode())
                            .withDescription(ycOp.getError().getMessage()),
                        tx);
                    tx.commit();
                }
            });

            metrics().cloneDiskError.inc();
            // don't restart
            return StepResult.FINISH;
        } catch (Exception e) {
            metrics().cloneDiskRetryableError.inc();
            log().error("Cannot complete failed ycCreateDisk operation {}/{}: {}. Reschedule...",
                opId(), state.ycCreateDiskOperationId(), e.getMessage());
            return StepResult.RESTART;
        }
    }

    private StepResult startDeleteSnapshot() {
        if (!state.ycDeleteSnapshotOperationId().isEmpty()) {
            return StepResult.ALREADY_DONE;
        }

        assert state.snapshotId() != null;

        OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycSnapshotService()
                .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId() + "-DeleteSnapshot"))
                .delete(
                    SnapshotServiceOuterClass.DeleteSnapshotRequest.newBuilder()
                        .setSnapshotId(state.snapshotId())
                        .build());
        } catch (StatusRuntimeException e) {
            log().error("Error while creating YcCloneDisk::DeleteSnapshot op {} state: [{}] {}. Reschedule...",
                opId(), e.getStatus().getCode(), e.getStatus().getDescription());

            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        failOperation(e.getStatus(), tx);
                        diskOpDao().deleteDiskOp(opId(), tx);
                        tx.commit();
                    }
                });

                metrics().cloneDiskError.inc();
            } catch (Exception ex) {
                metrics().cloneDiskRetryableError.inc();
                log().error("Error while failing YcCloneDisk::DeleteSnapshot op {}: {}. Reschedule...",
                    opId(), e.getMessage());
                return StepResult.RESTART;
            }

            return StepResult.FINISH;
        }

        InjectedFailures.failCloneDisk3();

        return saveState(
            () -> {
                state = state.withDeleteSnapshotOperationId(ycOp.getId());
                log().info("Wait YC at YcCloneDisk::DeleteSnapshot {}/{}...",
                    opId(), state.ycDeleteSnapshotOperationId());
            },
            () -> { }
        );
    }

    private StepResult waitCleanup() {
        log().info("Test status of YcCloneDisk::DeleteSnapshot operation {}/{}",
            opId(), state.ycDeleteSnapshotOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycDeleteSnapshotOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            log().error("Error while getting YcCloneDisk::DeleteSnapshot op {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycDeleteSnapshotOperationId(), e.getStatus().getCode(),
                e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            log().info("YcCloneDisk::DeleteSnapshot {}/{} not completed yet, reschedule...",
                opId(), state.ycDeleteSnapshotOperationId());
            return StepResult.RESTART;
        }

        if (ycOp.hasError()) {
            log().warn("YcCloneDisk::DeleteSnapshot operation {}/{} failed with error {}",
                opId(), state.ycCreateSnapshotOperationId(), ycOp.getError());
            try {
                withRetries(log(), () -> diskOpDao().failDiskOp(opId(), ycOp.getError().getMessage(), null));
            } catch (Exception e) {
                log().error("Cannot complete failed YcCloneDisk::DeleteSnapshot operation {}/{}: {}. Reschedule...",
                    opId(), state.ycDeleteSnapshotOperationId(), e.getMessage());
                return StepResult.RESTART;
            }

            // don't restart
            return StepResult.FINISH;
        }

        assert ycOp.hasResponse();

        log().warn("YcCloneDisk::DeleteSnapshot operation {}/{} successfully completed",
            opId(), state.ycDeleteSnapshotOperationId());

        try {
            withRetries(log(), () -> diskOpDao().deleteDiskOp(opId(), null));
            return StepResult.FINISH;
        } catch (Exception e) {
            log().debug("Cannot save new state for YcCloneDisk::DeleteSnapshot op {}/{} ({}), reschedule...",
                opId(), state.ycDeleteSnapshotOperationId(), state.snapshotId());
            return StepResult.RESTART;
        }
    }
}
