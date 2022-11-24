package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;

final class YcDeleteDiskAction implements Runnable {
    private static final Logger LOG = LogManager.getLogger(YcDeleteDiskAction.class);

    private final String opId;
    private YcDeleteDiskState state;
    private final AllocatorDataSource storage;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final OperationDao operationsDao;
    private final ScheduledExecutorService executor;
    private final ObjectMapper objectMapper;
    private final DiskServiceGrpc.DiskServiceBlockingStub ycDiskService;
    private final OperationServiceGrpc.OperationServiceBlockingStub ycOperationService;
    private boolean ycOpIdSaved;

    public YcDeleteDiskAction(String opId, YcDeleteDiskState state, AllocatorDataSource storage, DiskDao diskDao,
                              DiskOpDao diskOpDao, OperationDao operationsDao, ScheduledExecutorService executor,
                              ObjectMapper objectMapper, DiskServiceGrpc.DiskServiceBlockingStub ycDiskService,
                              OperationServiceGrpc.OperationServiceBlockingStub ycOperationService)
    {
        this.opId = opId;
        this.state = state;
        this.storage = storage;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.operationsDao = operationsDao;
        this.executor = executor;
        this.objectMapper = objectMapper;
        this.ycDiskService = ycDiskService;
        this.ycOperationService = ycOperationService;
        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
    }

    @Override
    public void run() {
        LOG.info("Deleting disk with id {}, outerOp {}", state.diskId(), opId);

        if (!ycOpIdSaved) {
            if (state.ycOperationId().isEmpty()) {
                try {
                    var ycDeleteDiskOperation = ycDiskService
                        .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId))
                        .delete(
                            DiskServiceOuterClass.DeleteDiskRequest.newBuilder()
                                .setDiskId(state.diskId())
                                .build());

                    state = state.withYcOperationId(ycDeleteDiskOperation.getId());
                } catch (StatusRuntimeException e) {
                    LOG.error("Error while running YcDeleteDisk op {} state: [{}] {}. Reschedule...",
                        opId, e.getStatus().getCode(), e.getStatus().getDescription());
                    executor.schedule(this, 2, TimeUnit.SECONDS);
                    return;
                }
            }

            try {
                withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
                ycOpIdSaved = true;
            } catch (Exception e) {
                LOG.debug("Cannot save new state for YcDeleteDisk {}/{}, reschedule...", opId, state.ycOperationId());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }

            LOG.info("Wait YC at YcDeleteDisk {}/{}...", opId, state.ycOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        LOG.info("Test status of YcDeleteDisk operation {}/{}", opId, state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService.get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcDeleteDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId, state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcDeleteDisk {}/{} not completed yet, reschedule...", opId, state.ycOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
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
                    try (var tx = TransactionHandle.create(storage)) {
                        diskOpDao.deleteDiskOp(opId, tx);
                        diskDao.remove(state.diskId(), tx);
                        operationsDao.updateMetaAndResponse(opId, meta, resp, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Cannot complete successful YcDeleteDisk operation {}/{}: {}. Reschedule...",
                    opId, state.ycOperationId(), e.getMessage());
                executor.schedule(this, 2, TimeUnit.SECONDS);
            }
            return;
        }

        try {
            LOG.warn("YcDeleteDisk failed with error {}", ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    diskOpDao.failDiskOp(opId, ycOp.getError().getMessage(), tx);
                    operationsDao.updateError(opId, ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("Cannot complete failed YcDeleteDisk operation {}/{}: {}. Reschedule...",
                opId, state.ycOperationId(), e.getMessage());
            executor.schedule(this, 2, TimeUnit.SECONDS);
        }
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
