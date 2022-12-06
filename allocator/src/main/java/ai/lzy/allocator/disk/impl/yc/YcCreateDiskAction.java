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
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

final class YcCreateDiskAction extends YcDiskActionBase<YcCreateDiskState> {
    private static final Logger LOG = LogManager.getLogger(YcCreateDiskAction.class);

    private boolean ycOpIdSaved;

    YcCreateDiskAction(DiskManager.OuterOperation op, YcCreateDiskState state, YcDiskManager diskManager) {
        super(op, state, diskManager);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
    }

    @Override
    public void run() {
        LOG.info("Creating disk with name = {} in compute, size = {}Gb, zone = {}, outerOpId = {}",
            state.spec().name(), state.spec().sizeGb(), state.spec().zone(), opId());

        var now = Instant.now();

        if (op.deadline().isBefore(now)) {
            LOG.error("YcCreateDisk operation {} expired", opId());
            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        operationsDao().failOperation(opId(), toProto(Status.DEADLINE_EXCEEDED), tx, LOG);
                        diskOpDao().deleteDiskOp(opId(), tx);
                        tx.commit();
                    }
                });

                metrics().createDiskError.inc();
                metrics().createDiskTimeout.inc();
            } catch (Exception e) {
                metrics().createDiskRetryableError.inc();
                LOG.error("Error while expiring YcCreateDisk op {}: {}. Reschedule...", opId(), e.getMessage());
                restart();
            }
            return;
        }

        if (!ycOpIdSaved) {
            if (state.ycOperationId().isEmpty()) {
                var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
                    .setName(state.spec().name())
                    .setFolderId(state.folderId())
                    .setSize(((long) state.spec().sizeGb()) << YcDiskManager.GB_SHIFT)
                    .setTypeId(state.spec().type().toYcName())
                    .setZoneId(state.spec().zone())
                    .putLabels(YcDiskManager.USER_ID_LABEL, state.meta().user());
                if (state.snapshotId() != null) {
                    diskRequestBuilder.setSnapshotId(state.snapshotId());
                }

                try {
                    var ycCreateDiskOperation = ycDiskService()
                        .withInterceptors(ClientHeaderInterceptor.idempotencyKey(this::opId))
                        .create(diskRequestBuilder.build());

                    state = state.withYcOperationId(ycCreateDiskOperation.getId());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                        LOG.warn("YcCreateDisk {}/{}, disk {} already exist", opId(),
                            state.ycOperationId(), state.spec().name());
                    } else {
                        LOG.error("Error while running YcCreateDisk op {} state: [{}] {}",
                            opId(), e.getStatus().getCode(), e.getStatus().getDescription());
                        try {
                            withRetries(LOG, () -> {
                                try (var tx = TransactionHandle.create(storage())) {
                                    operationsDao().failOperation(opId(), toProto(e.getStatus()), tx, LOG);
                                    diskOpDao().deleteDiskOp(opId(), tx);
                                    tx.commit();
                                }
                            });

                            metrics().createDiskError.inc();
                        } catch (Exception ex) {
                            LOG.error("Cannot fail YcCreateDisk op {}/{}: {}", opId(),
                                state.ycOperationId(), e.getMessage());
                            restart();
                        }
                        return;
                    }
                }
            }

            InjectedFailures.failCreateDisk1();

            try {
                withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
                ycOpIdSaved = true;
            } catch (Exception e) {
                metrics().createDiskRetryableError.inc();
                LOG.debug("Cannot save new state for YcCreateDisk {}/{}, reschedule...", opId(), state.ycOperationId());
                restart();
                return;
            }

            InjectedFailures.failCreateDisk2();

            LOG.info("Wait YC at YcCreateDisk {}/{}...", opId(), state.ycOperationId());
            restart();
            return;
        }

        LOG.info("Test status of YcCreateDisk operation {}/{}", opId(), state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().createDiskRetryableError.inc();
            LOG.error("Error while getting YcCreateDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCreateDisk {}/{} not completed yet, reschedule...", opId(), state.ycOperationId());
            restart();
            return;
        }

        if (ycOp.hasResponse()) {
            String diskId;
            try {
                diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
            } catch (InvalidProtocolBufferException e) {
                metrics().createDiskError.inc();
                LOG.error("Cannot complete successful YcCreateDisk operation {}/{}: {}",
                    opId(), state.ycOperationId(), e.getMessage());
                return;
            }

            try {
                var disk = new Disk(diskId, state.spec(), state.meta());

                LOG.info("YcCreateDisk op {} succeeded, created disk {}", opId(), disk);

                var meta = Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(diskId)
                            .build())
                    .toByteArray();

                var resp = Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build())
                    .toByteArray();

                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        diskDao().insert(disk, tx);
                        operationsDao().updateMetaAndResponse(opId(), meta, resp, tx);
                        tx.commit();
                    }
                });

                metrics().createDiskNewFinish.inc();
                metrics().createNewDiskDuration.observe(Duration.between(op.startedAt(), now).getSeconds());
            } catch (Exception e) {
                var sqlError = e instanceof SQLException;

                LOG.error("Cannot complete successful ycCreateDisk operation {}/{}: {}.{}",
                    opId(), state.ycOperationId(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

                if (sqlError) {
                    restart();
                }
            }
            return;
        }

        try {
            LOG.warn("YcCreateDisk op {} failed with error {}", opId(), ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    diskOpDao().deleteDiskOp(opId(), tx);
                    operationsDao().updateError(opId(), ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            LOG.error("Cannot complete failed ycCreateDisk operation {}/{}: {}.{}",
                opId(), state.ycOperationId(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

            if (sqlError) {
                restart();
            }
        }
    }
}
