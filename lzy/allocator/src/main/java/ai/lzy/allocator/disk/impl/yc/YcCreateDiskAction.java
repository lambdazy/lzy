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
import jakarta.annotation.Nullable;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.stream.Collectors.joining;

final class YcCreateDiskAction extends YcDiskActionBase<YcCreateDiskState> {
    private boolean ycOpIdSaved;

    YcCreateDiskAction(DiskManager.OuterOperation op, YcCreateDiskState state, YcDiskManager diskManager) {
        super(op, "[YcCreateDisk]", state, diskManager);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();

        log().info("Creating disk with name = {} for user {} in YC Compute, size = {}Gb, zone = {}, outerOpId = {}",
            state.spec().name(), state.meta().user(), state.spec().sizeGb(), state.spec().zone(), opId());
    }

    @Override
    protected void notifyExpired() {
        diskManager.metrics().createDiskError.inc();
        diskManager.metrics().createDiskTimeout.inc();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        diskOpDao().deleteDiskOp(opId(), tx);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::startDiskCreation, this::waitDiskCreation);
    }

    private StepResult startDiskCreation() {
        if (ycOpIdSaved) {
            return StepResult.ALREADY_DONE;
        }

        if (state.ycOperationId().isEmpty()) {
            var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
                .setName(state.spec().name())
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

                log().error("Error while running YcCreateDisk {} op {} state: [{}] {}",
                    state.spec().name(), opId(), status.getCode(), status.getDescription());

                if (status.getCode() == Status.Code.ALREADY_EXISTS) {
                    var ret = handleExistingDisk(state.spec().name());
                    switch (ret.code()) {
                        case CONTINUE -> { }
                        case RESTART, FINISH -> { return ret; }
                    }
                } else {
                    var ex = failOp(status);
                    if (ex != null) {
                        log().error("Cannot fail YcCreateDisk {} op {}/{}: {}",
                            state.spec().name(), opId(), state.ycOperationId(), e.getMessage());
                        return StepResult.RESTART;
                    }
                    return StepResult.FINISH;
                }
            }
        }

        InjectedFailures.failCreateDisk1();

        try {
            withRetries(log(), () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycOpIdSaved = true;
        } catch (Exception e) {
            metrics().createDiskRetryableError.inc();
            log().debug("Cannot save new state for YcCreateDisk {} op {}/{}, reschedule...",
                state.spec().name(), opId(), state.ycOperationId());
            return StepResult.RESTART;
        }

        InjectedFailures.failCreateDisk2();

        log().info("Wait YC at YcCreateDisk {} op {}/{}...", state.spec().name(), opId(), state.ycOperationId());
        return StepResult.RESTART;
    }

    private StepResult waitDiskCreation() {
        log().info("Test status of YcCreateDisk {} op {}/{}", state.spec().name(), opId(), state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().createDiskRetryableError.inc();
            log().error("Error while getting YcCreateDisk {} op {}/{} state: [{}] {}. Reschedule...",
                state.spec().name(), opId(), state.ycOperationId(), e.getStatus().getCode(),
                e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            log().debug("YcCreateDisk {} op {}/{} not completed yet, reschedule...",
                state.spec().name(), opId(), state.ycOperationId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        if (ycOp.hasResponse()) {
            String diskId;
            try {
                diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
            } catch (InvalidProtocolBufferException e) {
                log().error("Cannot complete successful YcCreateDisk {} op {}/{}: {}",
                    state.spec().name(), opId(), state.ycOperationId(), e.getMessage());

                var ex = failOp(Status.INTERNAL.withDescription("Cannot parse protobuf: " + e.getMessage()));
                if (ex != null) {
                    metrics().createDiskRetryableError.inc();
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }

            try {
                var disk = new Disk(diskId, state.spec(), state.meta());

                log().info("YcCreateDisk {} op {} succeeded, created disk {}", state.spec().name(), opId(), disk);

                var meta = Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(diskId)
                            .build());

                var resp = Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build());

                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        diskDao().insert(disk, tx);
                        completeOperation(meta, resp, tx);
                        tx.commit();
                    }
                });

                metrics().createDiskNewFinish.inc();
                metrics().createNewDiskDuration.observe(Duration.between(op.startedAt(), Instant.now()).getSeconds());

                return StepResult.FINISH;
            } catch (Exception e) {
                var sqlError = e instanceof SQLException;

                log().error("Cannot complete successful YcCreateDisk {} op {}/{}: {}.{}",
                    state.spec().name(), opId(), state.ycOperationId(), e.getMessage(),
                    (sqlError ? " Reschedule..." : ""));

                return sqlError ? StepResult.RESTART : StepResult.FINISH;
            }
        }

        if (ycOp.hasError()) {
            log().warn("YcCreateDisk {} op {} failed with error {}", state.spec().name(), opId(), ycOp.getError());

            var ex = failOp(ycOp.getError());
            if (ex != null) {
                log().error("Cannot complete failed YcCreateDisk {} op {}/{}: {}",
                    state.spec().name(), opId(), state.ycOperationId(), ex.getMessage());
                return StepResult.RESTART;
            }
        }

        assert false;
        return StepResult.FINISH;
    }

    @Nullable
    private Exception failOp(com.google.rpc.Status status) {
        return failOp(Status.fromCodeValue(status.getCode()).withDescription(status.getMessage()));
    }

    @Nullable
    private Exception failOp(Status status) {
        try {
            var done = withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    var ok = diskOpDao().deleteDiskOp(opId(), tx);
                    if (ok) {
                        failOperation(status, tx);
                        tx.commit();
                    }
                    return ok;
                }
            });

            if (done) {
                metrics().createDiskError.inc();

                switch (status.getCode()) {
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

    private StepResult handleExistingDisk(String diskName) {
        try {
            var disks = ycDiskService().list(
                DiskServiceOuterClass.ListDisksRequest.newBuilder()
                    .setFolderId(state.folderId())
                    .setFilter("name=\"%s\"".formatted(diskName))
                    .build());

            if (disks.getDisksCount() == 0) {
                log().error("Error while running YcCreateDisk {} op {}, disk exists, but not observed",
                    opId(), diskName);
                return StepResult.RESTART;
            }

            log().info("YcCreateDisk {} op {}: found existing disks: {}", diskName, opId(),
                disks.getDisksList().stream()
                    .map(disk -> TextFormat.printer().shortDebugString(disk))
                    .collect(joining("; ", "[", "]")));

            assert disks.getDisksCount() == 1;

            var existingDisk = disks.getDisks(0);
            var existingDiskOp = existingDisk.getLabelsMap().get(YcDiskManager.LZY_OP_LABEL);

            if (!opId().equals(existingDiskOp)) {
                log().error("Cannot execute YcCreateDisk {} op {}:" +
                        " disk already exists and doesn't belong to the current operation ({})",
                    diskName, opId(), existingDiskOp);

                var ex = failOp(Status.ALREADY_EXISTS);
                if (ex != null) {
                    log().error("Cannot fail operation {} with status ALREADY_EXISTS: {}. Reschedule...",
                        opId(), ex.getMessage());
                    metrics().createDiskRetryableError.inc();
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }

            var ops = ycDiskService().listOperations(
                DiskServiceOuterClass.ListDiskOperationsRequest.newBuilder()
                    .setDiskId(existingDisk.getId())
                    .build());

            log().info("YcCreateDisk {} op {}: found operations for existing disk: {}", diskName, opId(),
                ops.getOperationsList().stream()
                    .map(op -> TextFormat.printer().shortDebugString(op))
                    .collect(joining(", ", "[", "]")));

            var ycOp = ops.getOperationsList().stream()
                .filter(op -> "Create disk".equals(op.getDescription()))
                .findFirst();

            if (ycOp.isPresent()) {
                state = state.withYcOperationId(ycOp.get().getId());
                // continue...
                return StepResult.CONTINUE;
            }

            log().error("YcCreateDisk {} op {}: cannot find yc operation", diskName, opId());
            return StepResult.FINISH;
        } catch (StatusRuntimeException ex) {
            log().error("YcCreateDisk {} op {}, retry ALREADY_EXISTS check", diskName, opId());
            return StepResult.RESTART;
        }
    }
}
