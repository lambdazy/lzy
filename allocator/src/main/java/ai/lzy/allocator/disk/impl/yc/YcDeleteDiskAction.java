package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import static ai.lzy.model.db.DbHelper.withRetries;

final class YcDeleteDiskAction extends YcDiskActionBase<YcDeleteDiskState> {
    private static final Logger LOG = LogManager.getLogger(YcDeleteDiskAction.class);

    private boolean ycOpIdSaved;

    public YcDeleteDiskAction(DiskManager.OuterOperation op, YcDeleteDiskState state, YcDiskManager diskManager) {
        super(op, state, diskManager);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
    }

    @Override
    public void run() {
        LOG.info("Deleting disk with id {}, outerOp {}", state.diskId(), opId());

        if (!ycOpIdSaved) {
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
                    LOG.error("Error while running YcDeleteDisk op {} state: [{}] {}. Reschedule...",
                        opId(), e.getStatus().getCode(), e.getStatus().getDescription());
                    restart();
                    return;
                }
            }

            try {
                withRetries(LOG, () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
                ycOpIdSaved = true;
            } catch (Exception e) {
                LOG.debug("Cannot save new state for YcDeleteDisk {}/{}, reschedule...", opId(), state.ycOperationId());
                restart();
                return;
            }

            LOG.info("Wait YC at YcDeleteDisk {}/{}...", opId(), state.ycOperationId());
            restart();
            return;
        }

        LOG.info("Test status of YcDeleteDisk operation {}/{}", opId(), state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService().get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcDeleteDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId(), state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcDeleteDisk {}/{} not completed yet, reschedule...", opId(), state.ycOperationId());
            restart();
            return;
        }

        // TODO: update DiskService metrics

        if (ycOp.hasResponse()) {
            LOG.info("YcDeleteDisk succeeded, removed disk {}", state.diskId());

            var meta = Any.pack(
                    DiskServiceApi.DeleteDiskMetadata.newBuilder().build())
                .toByteArray();

            var resp = Any.pack(
                    DiskServiceApi.DeleteDiskResponse.newBuilder().build())
                .toByteArray();

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage())) {
                        diskOpDao().deleteDiskOp(opId(), tx);
                        diskDao().remove(state.diskId(), tx);
                        operationsDao().updateMetaAndResponse(opId(), meta, resp, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Cannot complete successful YcDeleteDisk operation {}/{}: {}. Reschedule...",
                    opId(), state.ycOperationId(), e.getMessage());
                restart();
            }
            return;
        }

        try {
            LOG.warn("YcDeleteDisk failed with error {}", ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    diskOpDao().failDiskOp(opId(), ycOp.getError().getMessage(), tx);
                    operationsDao().updateError(opId(), ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("Cannot complete failed YcDeleteDisk operation {}/{}: {}. Reschedule...",
                opId(), state.ycOperationId(), e.getMessage());
            restart();
        }
    }
}
