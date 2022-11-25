package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.DiskServiceApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;

import static ai.lzy.model.db.DbHelper.withRetries;

final class YcCreateDiskAction extends YcDiskActionBase<YcCreateDiskState> {
    private static final Logger LOG = LogManager.getLogger(YcCreateDiskAction.class);

    private boolean ycOpIdSaved;

    YcCreateDiskAction(String opId, YcCreateDiskState state, AllocatorDataSource storage, DiskDao diskDao,
                       DiskOpDao diskOpDao, OperationDao operationsDao, ScheduledExecutorService executor,
                       ObjectMapper objectMapper, DiskServiceGrpc.DiskServiceBlockingStub ycDiskService,
                       OperationServiceGrpc.OperationServiceBlockingStub ycOperationService)
    {
        super(opId, state, storage, diskDao, diskOpDao, operationsDao, executor, objectMapper, ycDiskService,
            null, ycOperationService);

        this.ycOpIdSaved = !state.ycOperationId().isEmpty();
    }

    @Override
    public void run() {
        LOG.info("Creating disk with name = {} in compute, size = {}Gb, zone = {}, outerOpId = {}",
            state.spec().name(), state.spec().sizeGb(), state.spec().zone(), opId);

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
                    var ycCreateDiskOperation = ycDiskService
                        .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId))
                        .create(diskRequestBuilder.build());

                    state = state.withYcOperationId(ycCreateDiskOperation.getId());
                } catch (StatusRuntimeException e) {
                    LOG.error("Error while running YcCreateDisk op {} state: [{}] {}. Reschedule...",
                        opId, e.getStatus().getCode(), e.getStatus().getDescription());
                    restart();
                    return;
                }
            }

            try {
                withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
                ycOpIdSaved = true;
            } catch (Exception e) {
                LOG.debug("Cannot save new state for YcCreateDisk {}/{}, reschedule...", opId, state.ycOperationId());
                restart();
                return;
            }

            LOG.info("Wait YC at YcCreateDisk {}/{}...", opId, state.ycOperationId());
            restart();
            return;
        }

        LOG.info("Test status of YcCreateDisk operation {}/{}", opId, state.ycOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService.get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcCreateDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId, state.ycOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            restart();
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCreateDisk {}/{} not completed yet, reschedule...", opId, state.ycOperationId());
            restart();
            return;
        }

        // TODO: update DiskService metrics (createDiskNewFinish, createDiskError, createNewDiskDuration)

        if (ycOp.hasResponse()) {
            try {
                var diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
                var disk = new Disk(diskId, state.spec(), state.meta());

                LOG.info("YcCreateDisk succeeded, created disk {}", disk);

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
                    try (var tx = TransactionHandle.create(storage)) {
                        diskOpDao.deleteDiskOp(opId, tx);
                        diskDao.insert(disk, tx);
                        operationsDao.updateMetaAndResponse(opId, meta, resp, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                var sqlError = e instanceof SQLException;

                LOG.error("Cannot complete successful ycCreateDisk operation {}/{}: {}.{}",
                    opId, state.ycOperationId(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

                if (sqlError) {
                    restart();
                }
            }
            return;
        }

        try {
            LOG.warn("YcCreateDisk failed with error {}", ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    diskOpDao.deleteDiskOp(opId, tx);
                    operationsDao.updateError(opId, ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            LOG.error("Cannot complete failed ycCreateDisk operation {}/{}: {}.{}",
                opId, state.ycOperationId(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

            if (sqlError) {
                restart();
            }
        }
    }
}
