package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
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
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.stream.Collectors.joining;

final class YcCreateDiskAction extends YcDiskActionBase<YcCreateDiskState> {
    private static final Logger LOG = LogManager.getLogger(YcCreateDiskAction.class);

    private static final Pattern DISK_NAME_PATTERN = Pattern.compile("[a-z]([-a-z0-9]{0,61}[a-z0-9])?");

    private boolean ycOpIdSaved;

    YcCreateDiskAction(DiskManager.OuterOperation op, YcCreateDiskState state, YcDiskManager diskManager) {
        super(op, state, diskManager);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
    }

    @Nullable
    @Override
    Exception failOp(com.google.rpc.Status status) {
        try {
            var done = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    var ok = diskOpDao().deleteDiskOp(opId(), tx);
                    if (ok) {
                        operationsDao().fail(opId(), status, tx);
                        tx.commit();
                    }
                    return ok;
                }
            });

            if (done) {
                metrics().createDiskError.inc();

                switch (Status.fromCodeValue(status.getCode()).getCode()) {
                    case ALREADY_EXISTS -> metrics().createDiskAlreadyExists.inc();
                    case DEADLINE_EXCEEDED -> metrics().createDiskTimeout.inc();
                }
            }

            return null;
        } catch (Exception e) {
            metrics().createDiskRetryableError.inc();
            return e;
        }
    }

    @Override
    public void run() {
        var diskName = state.spec().name();

        LOG.info("Creating disk with name = {} for user {} in YC Compute, size = {}Gb, zone = {}, outerOpId = {}",
            diskName, state.meta().user(), state.spec().sizeGb(), state.spec().zone(), opId());

        // Name of the disk. Value must match the regular expression [a-z]([-a-z0-9]{0,61}[a-z0-9])?.
        if (!DISK_NAME_PATTERN.matcher(diskName).matches()) {
            LOG.error("Disk name {} doesn't match pattern {}", diskName, DISK_NAME_PATTERN.pattern());
            var ex = failOp(Status.INVALID_ARGUMENT.withDescription("Invalid disk name"));
            if (ex != null) {
                restart();
            }
            return;
        }


        var now = Instant.now();

        if (op.deadline().isBefore(now)) {
            LOG.error("YcCreateDisk {} operation {} expired at {}", diskName, opId(), op.deadline());
            var ex = failOp(Status.DEADLINE_EXCEEDED);
            if (ex != null) {
                LOG.error("Error while expiring YcCreateDisk op {}: {}. Reschedule...", opId(), ex.getMessage());
                restart();
            }
            return;
        }

        if (!ycOpIdSaved) {
            if (state.ycOperationId().isEmpty()) {
                var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
                    .setName(diskName)
                    .setFolderId(state.folderId())
                    .setSize(((long) state.spec().sizeGb()) << YcDiskManager.GB_SHIFT)
                    .setTypeId(state.spec().type().toYcName())
                    .setZoneId(state.spec().zone())
                    .putLabels(YcDiskManager.USER_ID_LABEL, state.meta().user())
                    .putLabels(YcDiskManager.LZY_OP_LABEL, opId());
                if (state.snapshotId() != null) {
                    diskRequestBuilder.setSnapshotId(state.snapshotId());
                }

                try {
                    var ycCreateDiskOperation = ycDiskService()
                        .withInterceptors(ClientHeaderInterceptor.idempotencyKey(this::opId))
                        .create(diskRequestBuilder.build());

                    state = state.withYcOperationId(ycCreateDiskOperation.getId());
                } catch (StatusRuntimeException e) {
                    var status = e.getStatus();

                    LOG.error("Error while running YcCreateDisk {} op {} state: [{}] {}",
                        diskName, opId(), status.getCode(), status.getDescription());

                    if (status.getCode() == Status.Code.ALREADY_EXISTS) {
                        if (handleExistingDisk(diskName)) {
                            return;
                        }
                    } else {
                        var ex = failOp(status);
                        if (ex != null) {
                            LOG.error("Cannot fail YcCreateDisk {} op {}/{}: {}",
                                diskName, opId(), state.ycOperationId(), e.getMessage());
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
                LOG.debug("Cannot save new state for YcCreateDisk {} op {}/{}, reschedule...",
                    diskName, opId(), state.ycOperationId());
                restart();
                return;
            }

            InjectedFailures.failCreateDisk2();

            LOG.info("Wait YC at YcCreateDisk {} op {}/{}...", diskName, opId(), state.ycOperationId());
            restart();
            return;
        }

        LOG.info("Test status of YcCreateDisk {} op {}/{}", diskName, opId(), state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().createDiskRetryableError.inc();
            LOG.error("Error while getting YcCreateDisk {} op {}/{} state: [{}] {}. Reschedule...",
                diskName, opId(), state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCreateDisk {} op {}/{} not completed yet, reschedule...",
                diskName, opId(), state.ycOperationId());
            restart();
            return;
        }

        if (ycOp.hasResponse()) {
            String diskId;
            try {
                diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot complete successful YcCreateDisk {} op {}/{}: {}",
                    diskName, opId(), state.ycOperationId(), e.getMessage());

                var ex = failOp(Status.INTERNAL.withDescription("Cannot parse protobuf: " + e.getMessage()));
                if (ex != null) {
                    metrics().createDiskRetryableError.inc();
                    restart();
                }

                return;
            }

            try {
                var disk = new Disk(diskId, state.spec(), state.meta());

                LOG.info("YcCreateDisk {} op {} succeeded, created disk {}", diskName, opId(), disk);

                var meta = Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(diskId)
                            .build());

                var resp = Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build());

                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        diskDao().insert(disk, tx);
                        operationsDao().complete(opId(), meta, resp, tx);
                        tx.commit();
                    }
                });

                metrics().createDiskNewFinish.inc();
                metrics().createNewDiskDuration.observe(Duration.between(op.startedAt(), now).getSeconds());
            } catch (Exception e) {
                var sqlError = e instanceof SQLException;

                LOG.error("Cannot complete successful YcCreateDisk {} op {}/{}: {}.{}",
                    diskName, opId(), state.ycOperationId(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

                if (sqlError) {
                    restart();
                }
            }
            return;
        }

        LOG.warn("YcCreateDisk {} op {} failed with error {}", diskName, opId(), ycOp.getError());

        var ex = failOp(ycOp.getError());
        if (ex != null) {
            LOG.error("Cannot complete failed YcCreateDisk {} op {}/{}: {}",
                diskName, opId(), state.ycOperationId(), ex.getMessage());
            restart();
        }
    }

    private boolean handleExistingDisk(String diskName) {
        try {
            var disks = ycDiskService().list(
                DiskServiceOuterClass.ListDisksRequest.newBuilder()
                    .setFolderId(state.folderId())
                    .setFilter("name=\"%s\"".formatted(diskName))
                    .build());

            if (disks.getDisksCount() == 0) {
                LOG.error("Error while running YcCreateDisk {} op {}, disk exists, but not observed",
                    opId(), diskName);
                restart();
                return true;
            }

            LOG.info("YcCreateDisk {} op {}: found existing disks: {}", diskName, opId(),
                disks.getDisksList().stream()
                    .map(disk -> TextFormat.printer().shortDebugString(disk))
                    .collect(joining("; ", "[", "]")));

            assert disks.getDisksCount() == 1;

            var existingDisk = disks.getDisks(0);
            var existingDiskOp = existingDisk.getLabelsMap().get(YcDiskManager.LZY_OP_LABEL);

            if (!opId().equals(existingDiskOp)) {
                LOG.error("Cannot execute YcCreateDisk {} op {}:" +
                        " disk already exists and doesn't belong to the current operation ({})",
                    diskName, opId(), existingDiskOp);

                var ex = failOp(Status.ALREADY_EXISTS);
                if (ex != null) {
                    LOG.error("Cannot fail operation {} with status ALREADY_EXISTS: {}. Reschedule...",
                        opId(), ex.getMessage());
                    metrics().createDiskRetryableError.inc();
                    restart();
                }

                return true;
            }

            var ops = ycDiskService().listOperations(
                DiskServiceOuterClass.ListDiskOperationsRequest.newBuilder()
                    .setDiskId(existingDisk.getId())
                    .build());

            LOG.info("YcCreateDisk {} op {}: found operations for existing disk: {}", diskName, opId(),
                ops.getOperationsList().stream()
                    .map(op -> TextFormat.printer().shortDebugString(op))
                    .collect(joining(", ", "[", "]")));

            var ycOp = ops.getOperationsList().stream()
                .filter(op -> "Create disk".equals(op.getDescription()))
                .findFirst();

            if (ycOp.isPresent()) {
                state = state.withYcOperationId(ycOp.get().getId());
                // continue...
                return false;
            }

            LOG.error("YcCreateDisk {} op {}: cannot find yc operation", diskName, opId());
            return true;
        } catch (StatusRuntimeException ex) {
            LOG.error("YcCreateDisk {} op {}, retry ALREADY_EXISTS check", diskName, opId());
            restart();
            return true;
        }
    }
}
