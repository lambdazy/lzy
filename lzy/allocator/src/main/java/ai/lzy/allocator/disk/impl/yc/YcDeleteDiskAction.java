package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class YcDeleteDiskAction extends YcDiskActionBase<YcDeleteDiskState> {
    private boolean ycOpIdSaved;

    public YcDeleteDiskAction(DiskManager.OuterOperation op, YcDeleteDiskState state, YcDiskManager diskManager) {
        super(op, "[YcDeleteDisk]", state, diskManager);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
        log().info("Deleting disk with id {}, outerOp {}", state.diskId(), opId());
    }

    @Override
    protected void notifyExpired() {
        metrics().deleteDiskError.inc();
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        diskOpDao().deleteDiskOp(opId(), tx);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::startDeleteDisk, this::waitDeleteDisk);
    }

    private StepResult startDeleteDisk() {
        if (ycOpIdSaved) {
            return StepResult.CONTINUE;
        }

        if (state.ycOperationId().isEmpty()) {
            try {
                var ycDeleteDiskOperation = ycDiskService()
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(this::opId))
                    .delete(
                        DiskServiceOuterClass.DeleteDiskRequest.newBuilder()
                            .setDiskId(state.diskId())
                            .build());

                state = state.withYcOperationId(ycDeleteDiskOperation.getId());
            } catch (StatusRuntimeException e) {
                log().error("Error while running YcDeleteDisk op {} state: [{}] {}. Reschedule...",
                    opId(), e.getStatus().getCode(), e.getStatus().getDescription());

                try {
                    withRetries(log(), () -> {
                        try (var tx = TransactionHandle.create(storage())) {
                            failOperation(e.getStatus(), tx);
                            diskOpDao().deleteDiskOp(opId(), tx);
                            tx.commit();
                        }
                    });

                    metrics().deleteDiskError.inc();
                } catch (Exception ex) {
                    metrics().deleteDiskRetryableError.inc();
                    log().error("Error while failing YcDeleteDisk op {}: {}. Reschedule...", opId(), e.getMessage());
                    return StepResult.RESTART;
                }

                return StepResult.FINISH;
            }
        }

        InjectedFailures.failDeleteDisk1();

        try {
            withRetries(log(), () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            ycOpIdSaved = true;
        } catch (Exception e) {
            metrics().deleteDiskRetryableError.inc();
            log().debug("Cannot save new state for YcDeleteDisk {}/{}, reschedule...", opId(), state.ycOperationId());
            return StepResult.RESTART;
        }

        InjectedFailures.failDeleteDisk2();

        log().info("Wait YC at YcDeleteDisk {}/{}...", opId(), state.ycOperationId());
        return StepResult.RESTART.after(Duration.ofMillis(500));
    }

    private StepResult waitDeleteDisk() {
        log().info("Test status of YcDeleteDisk operation {}/{}", opId(), state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            metrics().deleteDiskRetryableError.inc();
            log().error("Error while getting YcDeleteDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            return StepResult.RESTART;
        }

        if (!ycOp.getDone()) {
            log().debug("YcDeleteDisk {}/{} not completed yet, reschedule...", opId(), state.ycOperationId());
            return StepResult.RESTART;
        }

        if (ycOp.hasResponse()) {
            log().info("YcDeleteDisk succeeded, removed disk {}", state.diskId());

            var meta = Any.pack(DiskServiceApi.DeleteDiskMetadata.newBuilder().build());
            var resp = Any.pack(DiskServiceApi.DeleteDiskResponse.newBuilder().build());

            try {
                withRetries(log(), () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        diskDao().remove(state.diskId(), tx);
                        completeOperation(meta, resp, tx);
                        tx.commit();
                    }
                });

                metrics().deleteDiskFinish.inc();
                metrics().deleteDiskDuration.observe(Duration.between(op.startedAt(), Instant.now()).getSeconds());
            } catch (Exception e) {
                metrics().deleteDiskRetryableError.inc();
                log().error("Cannot complete successful YcDeleteDisk operation {}/{}: {}. Reschedule...",
                    opId(), state.ycOperationId(), e.getMessage());
                return StepResult.RESTART;
            }
            return StepResult.FINISH;
        }

        try {
            log().warn("YcDeleteDisk failed with error {}", ycOp.getError());

            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    diskOpDao().failDiskOp(opId(), ycOp.getError().getMessage(), tx);
                    failOperation(
                        Status.fromCodeValue(ycOp.getError().getCode())
                            .withDescription(ycOp.getError().getMessage()),
                        tx);
                    tx.commit();
                }
            });

            metrics().deleteDiskError.inc();
            return StepResult.FINISH;
        } catch (Exception e) {
            metrics().deleteDiskRetryableError.inc();
            log().error("Cannot complete failed YcDeleteDisk operation {}/{}: {}. Reschedule...",
                opId(), state.ycOperationId(), e.getMessage());
            return StepResult.RESTART;
        }
    }
}
