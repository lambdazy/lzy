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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger LOG = LogManager.getLogger(YcCloneDiskAction.class);

    private boolean ycCreateSnapshotOpIdSaved;
    private boolean snapshotIdSaved;
    private boolean ycCreateDiskOpIdSaved;
    private boolean newDiskIdSaved;
    private boolean ycDeleteSnapshotOpIdSaved;
    private boolean snapshotRemoved;

    public YcCloneDiskAction(DiskManager.OuterOperation op, YcCloneDiskState state, YcDiskManager diskManager) {
        super(op, "[YcCloneDisk]", state, diskManager);

        this.ycCreateSnapshotOpIdSaved = !state.ycCreateSnapshotOperationId().isEmpty();
        this.snapshotIdSaved = state.snapshotId() != null;
        this.ycCreateDiskOpIdSaved = !state.ycCreateDiskOperationId().isEmpty();
        this.newDiskIdSaved = state.newDiskId() != null;
        this.ycDeleteSnapshotOpIdSaved = !state.ycDeleteSnapshotOperationId().isEmpty();
        this.snapshotRemoved = false;

        LOG.info("Clone disk {}; clone name={} size={}Gb zone={}, outerOp={}",
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
        diskOpDao().deleteDiskOp(opId(), tx);
        // TODO: remove snapshot on error
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::startCreateSnapshot, this::waitSnapshot, this::startCreateDisk, this::waitDisk,
            // TODO: emit new operation
            this::startDeleteSnapshot, this::waitCleanup);
    }

    private StepResult startCreateSnapshot() {
        if (ycCreateSnapshotOpIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        if (state.ycCreateSnapshotOperationId().isEmpty()) {
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
                state = state.withCreateSnapshotOperationId(ycOp.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while creating YcCloneDisk::CreateSnapshot op {} state: [{}] {}",
                    opId(), e.getStatus().getCode(), e.getStatus().getDescription());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            failOperation(e.getStatus(), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::CreateSnapshot op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }
        }

        InjectedFailures.failCloneDisk1();

        try {
            withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycCreateSnapshotOpIdSaved = true;
        } catch (Exception e) {
            metrics().cloneDiskRetryableError.inc();
            LOG.debug("Cannot save new state for YcCloneDisk::CreateSnapshot {}/{}, reschedule...",
                opId(), state.ycCreateSnapshotOperationId());
            return StepResult.RESTART;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateSnapshot {}/{}...", opId(), state.ycCreateSnapshotOperationId());
        return StepResult.RESTART;
    }

    private StepResult waitSnapshot() {
        if (snapshotIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        if (state.snapshotId() == null) {
            LOG.info("Test status of YcCloneDisk::CreateSnapshot operation {}/{}",
                opId(), state.ycCreateSnapshotOperationId());

            final OperationOuterClass.Operation ycGetSnapshotOp;
            try {
                ycGetSnapshotOp = ycOperationService().get(
                    OperationServiceOuterClass.GetOperationRequest.newBuilder()
                        .setOperationId(state.ycCreateSnapshotOperationId())
                        .build());
            } catch (StatusRuntimeException e) {
                metrics().cloneDiskRetryableError.inc();
                LOG.error("Error while getting YcCloneDisk::CreateSnapshot op {}/{} state: [{}] {}. Reschedule...",
                    opId(), state.ycCreateSnapshotOperationId(), e.getStatus().getCode(),
                    e.getStatus().getDescription());
                return StepResult.RESTART;
            }

            if (!ycGetSnapshotOp.getDone()) {
                LOG.info("YcCloneDisk::CreateSnapshot {}/{} not completed yet, reschedule...",
                    opId(), state.ycCreateSnapshotOperationId());
                return StepResult.RESTART;
            }

            if (ycGetSnapshotOp.hasError()) {
                LOG.warn("YcCloneDisk::CreateSnapshot operation {}/{} failed with error {}",
                    opId(), state.ycCreateSnapshotOperationId(), ycGetSnapshotOp.getError());
                try {
                    withRetries(LOG, () -> {
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
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot operation {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    return StepResult.RESTART;
                }

                // don't restart
                return StepResult.FINISH;
            }

            assert ycGetSnapshotOp.hasResponse();

            LOG.warn("YcCloneDisk::CreateSnapshot operation {}/{} successfully completed",
                opId(), state.ycCreateSnapshotOperationId());

            try {
                var snapshotId = ycGetSnapshotOp.getMetadata()
                    .unpack(SnapshotServiceOuterClass.CreateSnapshotMetadata.class).getSnapshotId();
                state = state.withSnapshotId(snapshotId);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateSnapshotMetadata, YcCloneDisk::CreateSnapshot {}/{} failed",
                    opId(), state.ycCreateSnapshotOperationId(), e);
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            diskOpDao().failDiskOp(opId(), e.getMessage(), tx);
                            failOperation(Status.INTERNAL.withDescription(e.getMessage()), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    return StepResult.RESTART;
                }

                metrics().cloneDiskError.inc();
                // don't restart
                return StepResult.FINISH;
            }
        }

        try {
            withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            snapshotIdSaved = true;
            return StepResult.CONTINUE;
        } catch (Exception e) {
            metrics().cloneDiskRetryableError.inc();
            LOG.debug("Cannot save new state for YcCloneDisk::CreateSnapshot op {}/{} ({}), reschedule...",
                opId(), state.ycCreateSnapshotOperationId(), state.snapshotId());
            return StepResult.RESTART;
        }
    }

    private StepResult startCreateDisk() {
        if (ycCreateDiskOpIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        assert state.snapshotId() != null;

        if (state.ycCreateDiskOperationId().isEmpty()) {
            var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
                .setName(state.newDiskSpec().name())
                .setDescription("Cloned disk '%s'".formatted(state.originDisk().id()))
                .setFolderId(state.folderId())
                .setSize(((long) state.newDiskSpec().sizeGb()) << YcDiskManager.GB_SHIFT)
                .setTypeId(state.newDiskSpec().type().toYcName())
                .setZoneId(state.newDiskSpec().zone())
                .putLabels(YcDiskManager.USER_ID_LABEL, state.newDiskMeta().user())
                .setSnapshotId(state.snapshotId());

            try {
                var ycCreateDiskOperation = ycDiskService()
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId() + "-CreateDisk"))
                    .create(diskRequestBuilder.build());

                state = state.withCreateDiskOperationId(ycCreateDiskOperation.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while running YcCloneDisk::CreateDisk op {} state: [{}] {}. Reschedule...",
                    opId(), e.getStatus().getCode(), e.getStatus().getDescription());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            failOperation(e.getStatus(), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::CreateDisk op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }
        }

        InjectedFailures.failCloneDisk2();

        try {
            withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycCreateDiskOpIdSaved = true;
        } catch (Exception e) {
            metrics().cloneDiskRetryableError.inc();
            LOG.debug("Cannot save new state for YcCloneDisk::CreateDisk {}/{}, reschedule...",
                opId(), state.ycCreateDiskOperationId());
            return StepResult.RESTART;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateDisk {}/{}...", opId(), state.ycCreateDiskOperationId());
        return StepResult.RESTART;
    }

    private StepResult waitDisk() {
        if (newDiskIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        LOG.info("Test status of YcCloneDisk::CreateDisk operation {}/{}", opId(), state.ycCreateDiskOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycCreateDiskOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().cloneDiskRetryableError.inc();
            LOG.error("Error while getting YcCloneDisk::CreateDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycCreateDiskOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCloneDisk::CreateDisk {}/{} not completed yet, reschedule...",
                opId(), state.ycCreateDiskOperationId());
            return StepResult.RESTART;
        }

        if (ycOp.hasResponse()) {
            final Disk newDisk;
            try {
                var diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
                newDisk = new Disk(diskId, state.newDiskSpec(), state.newDiskMeta());

                LOG.info("YcCloneDisk::CreateDisk op {}/{} succeeded, created disk {}",
                    opId(), state.ycCreateDiskOperationId(), newDisk);

                state = state.withNewDiskId(diskId);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateDiskMetadata, YcCloneDisk::CreateDisk {}/{} failed",
                    opId(), state.ycCreateDiskOperationId(), e);
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            diskOpDao().failDiskOp(opId(), e.getMessage(), tx);
                            failOperation(Status.INTERNAL.withDescription(e.getMessage()), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
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
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().updateDiskOp(opId(), toJson(state), tx);
                        diskDao().insert(newDisk, tx);
                        completeOperation(meta, resp, tx);
                        tx.commit();
                    }
                });

                newDiskIdSaved = true;
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                LOG.error("Cannot complete successful YcCloneDisk::CreateDisk operation {}/{}: {}. Reschedule...",
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
            LOG.warn("YcCloneDisk::CreateDisk op {}/{} failed with error {}",
                opId(), state.ycCreateDiskOperationId(), ycOp.getError());

            withRetries(LOG, () -> {
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
            LOG.error("Cannot complete failed ycCreateDisk operation {}/{}: {}. Reschedule...",
                opId(), state.ycCreateDiskOperationId(), e.getMessage());
            return StepResult.RESTART;
        }
    }

    private StepResult startDeleteSnapshot() {
        if (ycDeleteSnapshotOpIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        assert state.snapshotId() != null;

        if (state.ycDeleteSnapshotOperationId().isEmpty()) {
            try {
                var ycOp = ycSnapshotService()
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId() + "-DeleteSnapshot"))
                    .delete(
                        SnapshotServiceOuterClass.DeleteSnapshotRequest.newBuilder()
                            .setSnapshotId(state.snapshotId())
                            .build());
                state = state.withDeleteSnapshotOperationId(ycOp.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while creating YcCloneDisk::DeleteSnapshot op {} state: [{}] {}. Reschedule...",
                    opId(), e.getStatus().getCode(), e.getStatus().getDescription());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            failOperation(e.getStatus(), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::DeleteSnapshot op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }
        }

        InjectedFailures.failCloneDisk3();

        try {
            withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycDeleteSnapshotOpIdSaved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot {}/{}, reschedule...",
                opId(), state.ycDeleteSnapshotOperationId());
            return StepResult.RESTART;
        }

        LOG.info("Wait YC at YcCloneDisk::DeleteSnapshot {}/{}...", opId(), state.ycDeleteSnapshotOperationId());
        return StepResult.RESTART;
    }

    private StepResult waitCleanup() {
        if (snapshotRemoved) {
            return StepResult.FINISH;
        }

        LOG.info("Test status of YcCloneDisk::DeleteSnapshot operation {}/{}",
            opId(), state.ycDeleteSnapshotOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycDeleteSnapshotOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcCloneDisk::DeleteSnapshot op {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycDeleteSnapshotOperationId(), e.getStatus().getCode(),
                e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            LOG.info("YcCloneDisk::DeleteSnapshot {}/{} not completed yet, reschedule...",
                opId(), state.ycDeleteSnapshotOperationId());
            return StepResult.RESTART;
        }

        if (ycOp.hasError()) {
            LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} failed with error {}",
                opId(), state.ycCreateSnapshotOperationId(), ycOp.getError());
            try {
                withRetries(LOG, () -> diskOpDao().failDiskOp(opId(), ycOp.getError().getMessage(), null));
            } catch (Exception e) {
                LOG.error("Cannot complete failed YcCloneDisk::DeleteSnapshot operation {}/{}: {}. Reschedule...",
                    opId(), state.ycDeleteSnapshotOperationId(), e.getMessage());
                return StepResult.RESTART;
            }

            // don't restart
            return StepResult.FINISH;
        }

        assert ycOp.hasResponse();

        LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} successfully completed",
            opId(), state.ycDeleteSnapshotOperationId());

        try {
            withRetries(LOG, () -> diskOpDao().deleteDiskOp(opId(), null));
            snapshotRemoved = true;
            return StepResult.FINISH;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot op {}/{} ({}), reschedule...",
                opId(), state.ycDeleteSnapshotOperationId(), state.snapshotId());
            return StepResult.RESTART;
        }
    }
}
