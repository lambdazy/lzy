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

import java.time.Duration;
import java.time.Instant;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

final class YcCloneDiskAction extends YcDiskActionBase<YcCloneDiskState> {
    private static final Logger LOG = LogManager.getLogger(YcCloneDiskAction.class);

    private boolean ycCreateSnapshotOpIdSaved;
    private boolean snapshotIdSaved;
    private boolean ycCreateDiskOpIdSaved;
    private boolean newDiskIdSaved;
    private boolean ycDeleteSnapshotOpIdSaved;
    private boolean snapshotRemoved;

    public YcCloneDiskAction(DiskManager.OuterOperation op, YcCloneDiskState state, YcDiskManager diskManager) {
        super(op, state, diskManager);

        this.ycCreateSnapshotOpIdSaved = !state.ycCreateSnapshotOperationId().isEmpty();
        this.snapshotIdSaved = state.snapshotId() != null;
        this.ycCreateDiskOpIdSaved = !state.ycCreateDiskOperationId().isEmpty();
        this.newDiskIdSaved = state.newDiskId() != null;
        this.ycDeleteSnapshotOpIdSaved = !state.ycDeleteSnapshotOperationId().isEmpty();
        this.snapshotRemoved = false;
    }

    @Override
    public void run() {
        LOG.info("Clone disk {}; clone name={} size={}Gb zone={}, outerOp={}",
            state.originDisk().spec().name(), state.newDiskSpec().name(), state.newDiskSpec().sizeGb(),
            state.newDiskSpec().zone(), opId());

        var now = Instant.now();

        // TODO: remove snapshot on error

        if (op.deadline().isBefore(now)) {
            LOG.error("YcCloneDisk operation {} expired", opId());
            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        operationsDao().fail(opId(), toProto(Status.DEADLINE_EXCEEDED), tx);
                        diskOpDao().deleteDiskOp(opId(), tx);
                        tx.commit();
                    }
                });

                metrics().cloneDiskError.inc();
                metrics().cloneDiskTimeout.inc();
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                LOG.error("Error while expiring YcCloneDisk op {}: {}. Reschedule...", opId(), e.getMessage());
                restart();
            }
            return;
        }


        // CreateSnapshot operation
        if (!ycCreateSnapshotOpIdSaved) {
            startCreateSnapshot();
            return;
        }

        // Wait snapshot creation
        if (!snapshotIdSaved) {
            waitSnapshot();
            if (!snapshotIdSaved) {
                return;
            }
        }

        // CreateDisk operation
        if (!ycCreateDiskOpIdSaved) {
            startCreateDisk();
            return;
        }

        // Wait disk creation
        if (!newDiskIdSaved) {
            waitDisk();
            if (!newDiskIdSaved) {
                return;
            }
        }

        // DeleteSnapshot operation
        if (!ycDeleteSnapshotOpIdSaved) {
            deleteSnapshot();
            return;
        }

        // Wait snapshot removal
        if (!snapshotRemoved) {
            waitCleanup();
        }
    }

    private void startCreateSnapshot() {
        assert !ycCreateSnapshotOpIdSaved;

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
                            operationsDao().fail(opId(), toProto(e.getStatus()), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::CreateSnapshot op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    restart();
                }

                return;
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
            restart();
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateSnapshot {}/{}...", opId(), state.ycCreateSnapshotOperationId());
        restart();
    }

    private void waitSnapshot() {
        assert !snapshotIdSaved;

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
                restart();
                return;
            }

            if (!ycGetSnapshotOp.getDone()) {
                LOG.info("YcCloneDisk::CreateSnapshot {}/{} not completed yet, reschedule...",
                    opId(), state.ycCreateSnapshotOperationId());
                restart();
                return;
            }

            if (ycGetSnapshotOp.hasError()) {
                LOG.warn("YcCloneDisk::CreateSnapshot operation {}/{} failed with error {}",
                    opId(), state.ycCreateSnapshotOperationId(), ycGetSnapshotOp.getError());
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            diskOpDao().deleteDiskOp(opId(), tx);
                            operationsDao().fail(opId(), ycGetSnapshotOp.getError().toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception e) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot operation {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    restart();
                    return;
                }

                // don't restart
                return;
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
                            operationsDao().fail(opId(),
                                toProto(Status.INTERNAL.withDescription(e.getMessage())).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    restart();
                    return;
                }

                metrics().cloneDiskError.inc();
                // don't restart
                return;
            }

            try {
                withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
                snapshotIdSaved = true;
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                LOG.debug("Cannot save new state for YcCloneDisk::CreateSnapshot op {}/{} ({}), reschedule...",
                    opId(), state.ycCreateSnapshotOperationId(), state.snapshotId());
                restart();
            }
        }
    }

    private void startCreateDisk() {
        assert !ycCreateDiskOpIdSaved;
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
                            operationsDao().fail(opId(), toProto(e.getStatus()), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::CreateDisk op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    restart();
                }

                return;
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
            restart();
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateDisk {}/{}...", opId(), state.ycCreateDiskOperationId());
        restart();
    }

    private void waitDisk() {
        assert !newDiskIdSaved;

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
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCloneDisk::CreateDisk {}/{} not completed yet, reschedule...",
                opId(), state.ycCreateDiskOperationId());
            restart();
            return;
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
                            operationsDao().fail(opId(),
                                toProto(Status.INTERNAL.withDescription(e.getMessage())).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId(), state.ycCreateSnapshotOperationId(), e.getMessage());
                    restart();
                    return;
                }

                metrics().cloneDiskError.inc();
                // don't restart
                return;
            }

            var meta = Any.pack(
                    DiskServiceApi.CloneDiskMetadata.newBuilder()
                        .setDiskId(newDisk.id())
                        .build())
                .toByteArray();

            var resp = Any.pack(
                    DiskServiceApi.CloneDiskResponse.newBuilder()
                        .setDisk(newDisk.toProto())
                        .build())
                .toByteArray();

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().updateDiskOp(opId(), toJson(state), tx);
                        diskDao().insert(newDisk, tx);
                        operationsDao().complete(opId(), meta, resp, tx);
                        tx.commit();
                    }
                });

                newDiskIdSaved = true;
            } catch (Exception e) {
                metrics().cloneDiskRetryableError.inc();
                LOG.error("Cannot complete successful YcCloneDisk::CreateDisk operation {}/{}: {}. Reschedule...",
                    opId(), state.ycCreateDiskOperationId(), e.getMessage());
                restart();
                return;
            }

            metrics().cloneDiskFinish.inc();
            metrics().cloneDiskDuration.observe(Duration.between(op.startedAt(), Instant.now()).getSeconds());

            // don't restart
            return;
        }

        // CreateDisk failed

        try {
            LOG.warn("YcCloneDisk::CreateDisk op {}/{} failed with error {}",
                opId(), state.ycCreateDiskOperationId(), ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    diskOpDao().deleteDiskOp(opId(), tx);
                    operationsDao().fail(opId(), ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });

            metrics().cloneDiskError.inc();
            // don't restart
        } catch (Exception e) {
            metrics().cloneDiskRetryableError.inc();
            LOG.error("Cannot complete failed ycCreateDisk operation {}/{}: {}. Reschedule...",
                opId(), state.ycCreateDiskOperationId(), e.getMessage());
            restart();
        }
    }

    private void deleteSnapshot() {
        assert !ycDeleteSnapshotOpIdSaved;

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
                            operationsDao().fail(opId(), toProto(e.getStatus()), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().cloneDiskError.inc();
                } catch (Exception ex) {
                    metrics().cloneDiskRetryableError.inc();
                    LOG.error("Error while failing YcCloneDisk::DeleteSnapshot op {}: {}. Reschedule...",
                        opId(), e.getMessage());
                    restart();
                }

                return;
            }
        }

        InjectedFailures.failCloneDisk3();

        try {
            withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycDeleteSnapshotOpIdSaved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot {}/{}, reschedule...",
                opId(), state.ycDeleteSnapshotOperationId());
            restart();
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::DeleteSnapshot {}/{}...", opId(), state.ycDeleteSnapshotOperationId());
        restart();
    }

    private void waitCleanup() {
        assert !snapshotRemoved;

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
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.info("YcCloneDisk::DeleteSnapshot {}/{} not completed yet, reschedule...",
                opId(), state.ycDeleteSnapshotOperationId());
            restart();
            return;
        }

        if (ycOp.hasError()) {
            LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} failed with error {}",
                opId(), state.ycCreateSnapshotOperationId(), ycOp.getError());
            try {
                withRetries(LOG, () -> diskOpDao().failDiskOp(opId(), ycOp.getError().getMessage(), null));
            } catch (Exception e) {
                LOG.error("Cannot complete failed YcCloneDisk::DeleteSnapshot operation {}/{}: {}. Reschedule...",
                    opId(), state.ycDeleteSnapshotOperationId(), e.getMessage());
                restart();
                return;
            }

            // don't restart
            return;
        }

        assert ycOp.hasResponse();

        LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} successfully completed",
            opId(), state.ycDeleteSnapshotOperationId());

        try {
            withRetries(LOG, () -> diskOpDao().deleteDiskOp(opId(), null));
            snapshotRemoved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot op {}/{} ({}), reschedule...",
                opId(), state.ycDeleteSnapshotOperationId(), state.snapshotId());
            restart();
        }
    }
}
