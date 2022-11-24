package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.Disk;
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
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.DiskServiceOuterClass;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.compute.v1.SnapshotServiceOuterClass;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceOuterClass;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

final class YcCloneDiskAction implements Runnable {
    private static final Logger LOG = LogManager.getLogger(YcCloneDiskAction.class);

    private final String opId;
    private YcCloneDiskState state;
    private final AllocatorDataSource storage;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final OperationDao operationsDao;
    private final ScheduledExecutorService executor;
    private final ObjectMapper objectMapper;
    private final DiskServiceGrpc.DiskServiceBlockingStub ycDiskService;
    private final SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService;
    private final OperationServiceGrpc.OperationServiceBlockingStub ycOperationService;
    private boolean ycCreateSnapshotOpIdSaved;
    private boolean snapshotIdSaved;
    private boolean ycCreateDiskOpIdSaved;
    private boolean newDiskIdSaved;
    private boolean ycDeleteSnapshotOpIdSaved;
    private boolean snapshotRemoved;

    public YcCloneDiskAction(String opId, YcCloneDiskState state, AllocatorDataSource storage, DiskDao diskDao,
                             DiskOpDao diskOpDao, OperationDao operationsDao, ScheduledExecutorService executor,
                             ObjectMapper objectMapper, DiskServiceGrpc.DiskServiceBlockingStub ycDiskService,
                             SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService,
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
        this.ycSnapshotService = ycSnapshotService;
        this.ycOperationService = ycOperationService;
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
            state.newDiskSpec().zone(), opId);

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
                var ycOp = ycSnapshotService
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId))
                    .create(
                        SnapshotServiceOuterClass.CreateSnapshotRequest.newBuilder()
                            .setFolderId(state.folderId())
                            .setDiskId(state.originDisk().id())
                            .build());
                state = state.withCreateSnapshotOperationId(ycOp.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while creating YcCloneDisk::CreateSnapshot op {} state: [{}] {}. Reschedule...",
                    opId, e.getStatus().getCode(), e.getStatus().getDescription());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }
        }

        try {
            withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
            ycCreateSnapshotOpIdSaved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::CreateSnapshot {}/{}, reschedule...",
                opId, state.ycCreateSnapshotOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateSnapshot {}/{}...", opId, state.ycCreateSnapshotOperationId());
        executor.schedule(this, 2, TimeUnit.SECONDS);
    }

    private void waitSnapshot() {
        assert !snapshotIdSaved;

        if (state.snapshotId() == null) {
            LOG.info("Test status of YcCloneDisk::CreateSnapshot operation {}/{}",
                opId, state.ycCreateSnapshotOperationId());

            final OperationOuterClass.Operation ycGetSnapshotOp;
            try {
                ycGetSnapshotOp = ycOperationService.get(
                    OperationServiceOuterClass.GetOperationRequest.newBuilder()
                        .setOperationId(state.ycCreateSnapshotOperationId())
                        .build());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while getting YcCloneDisk::CreateSnapshot op {}/{} state: [{}] {}. Reschedule...",
                    opId, state.ycCreateSnapshotOperationId(), e.getStatus().getCode(),
                    e.getStatus().getDescription());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }

            if (!ycGetSnapshotOp.getDone()) {
                LOG.info("YcCloneDisk::CreateSnapshot {}/{} not completed yet, reschedule...",
                    opId, state.ycCreateSnapshotOperationId());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }

            if (ycGetSnapshotOp.hasError()) {
                LOG.warn("YcCloneDisk::CreateSnapshot operation {}/{} failed with error {}",
                    opId, state.ycCreateSnapshotOperationId(), ycGetSnapshotOp.getError());
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskOpDao.deleteDiskOp(opId, tx);
                            operationsDao.updateError(opId, ycGetSnapshotOp.getError().toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot operation {}/{}: {}. Reschedule...",
                        opId, state.ycCreateSnapshotOperationId(), e.getMessage());
                    executor.schedule(this, 2, TimeUnit.SECONDS);
                    return;
                }

                // don't restart
                return;
            }

            assert ycGetSnapshotOp.hasResponse();

            LOG.warn("YcCloneDisk::CreateSnapshot operation {}/{} successfully completed",
                opId, state.ycCreateSnapshotOperationId());

            try {
                var snapshotId = ycGetSnapshotOp.getMetadata()
                    .unpack(SnapshotServiceOuterClass.CreateSnapshotMetadata.class).getSnapshotId();
                state = state.withSnapshotId(snapshotId);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateSnapshotMetadata, YcCloneDisk::CreateSnapshot {}/{} failed",
                    opId, state.ycCreateSnapshotOperationId(), e);
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskOpDao.failDiskOp(opId, e.getMessage(), tx);
                            operationsDao.updateError(opId,
                                toProto(Status.INTERNAL.withDescription(e.getMessage())).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId, state.ycCreateSnapshotOperationId(), e.getMessage());
                    executor.schedule(this, 2, TimeUnit.SECONDS);
                    return;
                }

                // don't restart
                return;
            }

            try {
                withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
                snapshotIdSaved = true;
            } catch (Exception e) {
                LOG.debug("Cannot save new state for YcCloneDisk::CreateSnapshot op {}/{} ({}), reschedule...",
                    opId, state.ycCreateSnapshotOperationId(), state.snapshotId());
                executor.schedule(this, 2, TimeUnit.SECONDS);
            }
        }
    }

    private void startCreateDisk() {
        assert !ycCreateDiskOpIdSaved;
        assert state.snapshotId() != null;

        if (state.ycCreateDiskOperationId().isEmpty()) {
            var diskRequestBuilder = DiskServiceOuterClass.CreateDiskRequest.newBuilder()
                .setName(state.newDiskSpec().name())
                .setFolderId(state.folderId())
                .setSize(((long) state.newDiskSpec().sizeGb()) << YcDiskManager.GB_SHIFT)
                .setTypeId(state.newDiskSpec().type().toYcName())
                .setZoneId(state.newDiskSpec().zone())
                .putLabels(YcDiskManager.USER_ID_LABEL, state.newDiskMeta().user())
                .setSnapshotId(state.snapshotId());

            try {
                var ycCreateDiskOperation = ycDiskService
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId))
                    .create(diskRequestBuilder.build());

                state = state.withCreateDiskOperationId(ycCreateDiskOperation.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while running YcCloneDisk::CreateDisk op {} state: [{}] {}. Reschedule...",
                    opId, e.getStatus().getCode(), e.getStatus().getDescription());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }
        }

        try {
            withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
            ycCreateDiskOpIdSaved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::CreateDisk {}/{}, reschedule...",
                opId, state.ycCreateDiskOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::CreateDisk {}/{}...", opId, state.ycCreateDiskOperationId());
        executor.schedule(this, 2, TimeUnit.SECONDS);
    }

    private void waitDisk() {
        assert !newDiskIdSaved;

        LOG.info("Test status of YcCloneDisk::CreateDisk operation {}/{}", opId, state.ycCreateDiskOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService.get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycCreateDiskOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcCloneDisk::CreateDisk operation {}/{} state: [{}] {}. Reschedule...",
                opId, state.ycCreateDiskOperationId(), e.getStatus().getCode(), e.getStatus().getDescription());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        if (!ycOp.getDone()) {
            LOG.debug("YcCloneDisk::CreateDisk {}/{} not completed yet, reschedule...",
                opId, state.ycCreateDiskOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        // TODO: update DiskService metrics (createDiskNewFinish, createDiskError, createNewDiskDuration)

        if (ycOp.hasResponse()) {
            final Disk newDisk;
            try {
                var diskId = ycOp.getResponse().unpack(DiskOuterClass.Disk.class).getId();
                newDisk = new Disk(diskId, state.newDiskSpec(), state.newDiskMeta());

                LOG.info("YcCloneDisk::CreateDisk op {}/{} succeeded, created disk {}",
                    opId, state.ycCreateDiskOperationId(), newDisk);

                state = state.withNewDiskId(diskId);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse CreateDiskMetadata, YcCloneDisk::CreateDisk {}/{} failed",
                    opId, state.ycCreateDiskOperationId(), e);
                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskOpDao.failDiskOp(opId, e.getMessage(), tx);
                            operationsDao.updateError(opId,
                                toProto(Status.INTERNAL.withDescription(e.getMessage())).toByteArray(), tx);
                            tx.commit();
                        }
                    });
                } catch (Exception ex) {
                    LOG.error("Cannot complete failed YcCloneDisk::CreateSnapshot op {}/{}: {}. Reschedule...",
                        opId, state.ycCreateSnapshotOperationId(), e.getMessage());
                    executor.schedule(this, 2, TimeUnit.SECONDS);
                    return;
                }

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
                    try (var tx = TransactionHandle.create(storage)) {
                        diskOpDao.updateDiskOp(opId, toJson(state), tx);
                        diskDao.insert(newDisk, tx);
                        operationsDao.updateMetaAndResponse(opId, meta, resp, tx);
                        tx.commit();
                    }
                });

                newDiskIdSaved = true;
            } catch (Exception e) {
                LOG.error("Cannot complete successful YcCloneDisk::CreateDisk operation {}/{}: {}. Reschedule...",
                    opId, state.ycCreateDiskOperationId(), e.getMessage());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }

            // don't restart
            return;
        }

        // CreateDisk failed

        try {
            LOG.warn("YcCloneDisk::CreateDisk op {}/{} failed with error {}",
                opId, state.ycCreateDiskOperationId(), ycOp.getError());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    diskOpDao.deleteDiskOp(opId, tx);
                    operationsDao.updateError(opId, ycOp.getError().toByteArray(), tx);
                    tx.commit();
                }
            });

            // don't restart
        } catch (Exception e) {
            LOG.error("Cannot complete failed ycCreateDisk operation {}/{}: {}. Reschedule...",
                opId, state.ycCreateDiskOperationId(), e.getMessage());
            executor.schedule(this, 2, TimeUnit.SECONDS);
        }
    }

    private void deleteSnapshot() {
        assert !ycDeleteSnapshotOpIdSaved;

        if (state.ycDeleteSnapshotOperationId().isEmpty()) {
            try {
                var ycOp = ycSnapshotService
                    .withInterceptors(ClientHeaderInterceptor.idempotencyKey(() -> opId))
                    .delete(
                        SnapshotServiceOuterClass.DeleteSnapshotRequest.newBuilder()
                            .setSnapshotId(state.snapshotId())
                            .build());
                state = state.withDeleteSnapshotOperationId(ycOp.getId());
            } catch (StatusRuntimeException e) {
                LOG.error("Error while creating YcCloneDisk::DeleteSnapshot op {} state: [{}] {}. Reschedule...",
                    opId, e.getStatus().getCode(), e.getStatus().getDescription());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }
        }

        try {
            withRetries(LOG, () -> diskOpDao.updateDiskOp(opId, toJson(state), null));
            ycDeleteSnapshotOpIdSaved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot {}/{}, reschedule...",
                opId, state.ycDeleteSnapshotOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        LOG.info("Wait YC at YcCloneDisk::DeleteSnapshot {}/{}...", opId, state.ycDeleteSnapshotOperationId());
        executor.schedule(this, 2, TimeUnit.SECONDS);
    }

    private void waitCleanup() {
        assert !snapshotRemoved;

        LOG.info("Test status of YcCloneDisk::DeleteSnapshot operation {}/{}",
            opId, state.ycDeleteSnapshotOperationId());

        final OperationOuterClass.Operation ycOp;
        try {
            ycOp = ycOperationService.get(
                OperationServiceOuterClass.GetOperationRequest.newBuilder()
                    .setOperationId(state.ycDeleteSnapshotOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while getting YcCloneDisk::DeleteSnapshot op {}/{} state: [{}] {}. Reschedule...",
                opId, state.ycDeleteSnapshotOperationId(), e.getStatus().getCode(),
                e.getStatus().getDescription());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        if (!ycOp.getDone()) {
            LOG.info("YcCloneDisk::DeleteSnapshot {}/{} not completed yet, reschedule...",
                opId, state.ycDeleteSnapshotOperationId());
            executor.schedule(this, 2, TimeUnit.SECONDS);
            return;
        }

        if (ycOp.hasError()) {
            LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} failed with error {}",
                opId, state.ycCreateSnapshotOperationId(), ycOp.getError());
            try {
                withRetries(LOG, () -> diskOpDao.failDiskOp(opId, ycOp.getError().getMessage(), null));
            } catch (Exception e) {
                LOG.error("Cannot complete failed YcCloneDisk::DeleteSnapshot operation {}/{}: {}. Reschedule...",
                    opId, state.ycDeleteSnapshotOperationId(), e.getMessage());
                executor.schedule(this, 2, TimeUnit.SECONDS);
                return;
            }

            // don't restart
            return;
        }

        assert ycOp.hasResponse();

        LOG.warn("YcCloneDisk::DeleteSnapshot operation {}/{} successfully completed",
            opId, state.ycDeleteSnapshotOperationId());

        try {
            withRetries(LOG, () -> diskOpDao.deleteDiskOp(opId, null));
            snapshotRemoved = true;
        } catch (Exception e) {
            LOG.debug("Cannot save new state for YcCloneDisk::DeleteSnapshot op {}/{} ({}), reschedule...",
                opId, state.ycDeleteSnapshotOperationId(), state.snapshotId());
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
