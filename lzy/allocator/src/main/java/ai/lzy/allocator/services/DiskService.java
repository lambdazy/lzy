package ai.lzy.allocator.services;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskMetrics;
import ai.lzy.allocator.disk.DiskOperation;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.DiskApi;
import ai.lzy.v1.DiskServiceApi;
import ai.lzy.v1.DiskServiceApi.CloneDiskRequest;
import ai.lzy.v1.DiskServiceApi.CreateDiskRequest;
import ai.lzy.v1.DiskServiceApi.DeleteDiskRequest;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import lombok.Lombok;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.inject.Named;
import javax.inject.Singleton;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class DiskService extends DiskServiceGrpc.DiskServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(DiskService.class);

    private final ServiceConfig config;
    private final DiskManager diskManager;
    private final OperationDao operationsDao;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final AllocatorDataSource storage;
    private final OperationsExecutor executor;
    private final DiskMetrics metrics;

    public DiskService(ServiceConfig config, DiskManager diskManager, DiskDao diskDao, DiskOpDao diskOpDao,
                       AllocatorDataSource storage, @Named("AllocatorOperationDao") OperationDao operationDao,
                       @Named("AllocatorOperationsExecutor") OperationsExecutor executor, DiskMetrics metrics)
    {
        this.config = config;
        this.diskManager = diskManager;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.storage = storage;
        this.operationsDao = operationDao;
        this.executor = executor;
        this.metrics = metrics;

        restartActiveOperations();
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown DiskService, active requests: ???");
    }

    @VisibleForTesting
    public void restartActiveOperations() {
        final List<DiskOperation> ops;
        try {
            ops = diskOpDao.getActiveDiskOps(config.getInstanceId(), null);
        } catch (SQLException e) {
            LOG.error("Cannot load active disk operations: {}", e.getMessage());
            throw new RuntimeException(e);
        }

        if (ops.isEmpty()) {
            LOG.info("No active disk ops found for allocator instance '{}'", config.getInstanceId());
            return;
        }

        LOG.warn("Found {} not completed disk operations on allocator '{}'", ops.size(), config.getInstanceId());
        for (var op : ops) {
            LOG.info("Restore {}", op);
            op = diskManager.restoreDiskOperation(op);
            executor.startNew(op.deferredAction());
        }
    }

    @Override
    public void createDisk(CreateDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        if (!validateRequest(request, response)) {
            return;
        }

        LOG.info("Create disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, response, LOG)) {
            return;
        }

        final var createDiskOperation = Operation.create(
            request.getUserId(),
            "CreateDisk: " +
                (request.hasExistingDisk()
                    ? "from " + request.getExistingDisk().getDiskId()
                    : "new " + request.getDiskSpec()),
            Duration.ofHours(1),
            idempotencyKey,
            DiskServiceApi.CreateDiskMetadata.newBuilder().build());

        try {
            var diskOperation = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationsDao.create(createDiskOperation, tx);

                    var ctx = DiskManager.OuterOperation.from(createDiskOperation);

                    switch (request.getDiskDescriptionCase()) {
                        case DISK_SPEC -> {
                            var diskOp = diskManager.newCreateDiskOperation(
                                ctx, DiskSpec.fromProto(request.getDiskSpec()), new DiskMeta(request.getUserId()));

                            diskOpDao.createDiskOp(diskOp, tx);
                            tx.commit();
                            metrics.createDiskNewStart.inc();
                            return diskOp;
                        }

                        case EXISTING_DISK -> {
                            final String diskId = request.getExistingDisk().getDiskId();
                            Disk disk;
                            try {
                                disk = diskManager.get(diskId);
                            } catch (StatusException e) {
                                throw e.getStatus().asRuntimeException();
                            }
                            if (disk == null) {
                                LOG.error("Create disk failed, disk {} not found", diskId);

                                var status = toProto(Status.NOT_FOUND);
                                operationsDao.fail(createDiskOperation.id(), status, tx);

                                tx.commit();
                                metrics.createDiskError.inc();
                                return null;
                            }

                            var meta = Any.pack(
                                DiskServiceApi.CreateDiskMetadata.newBuilder()
                                    .setDiskId(diskId)
                                    .build());
                            var resp = Any.pack(
                                DiskServiceApi.CreateDiskResponse.newBuilder()
                                    .setDisk(disk.toProto())
                                    .build());

                            createDiskOperation.modifyMeta(meta);
                            createDiskOperation.completeWith(resp);

                            operationsDao.complete(createDiskOperation.id(), meta, resp, tx);

                            diskDao.insert(disk, tx);
                            tx.commit();
                            metrics.createDiskExisting.inc();
                            return null;
                        }
                    }

                    return null;
                } catch (InvalidConfigurationException e) {
                    throw Lombok.sneakyThrow(e);
                }
            });

            if (diskOperation != null) {
                InjectedFailures.failCreateDisk0();
                executor.startNew(diskOperation.deferredAction());
            }
        } catch (InvalidConfigurationException e) {
            LOG.error("Cannot create disk for owner {}: {}", request.getUserId(), e.getMessage());
            metrics.createDiskError.inc();
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationsDao, response, LOG))
            {
                return;
            }

            metrics.createDiskError.inc();
            LOG.error("Cannot create disk for owner {}: {}", request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        response.onNext(createDiskOperation.toProto());
        response.onCompleted();
    }

    @Override
    public void cloneDisk(CloneDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        if (!validateRequest(request, response)) {
            return;
        }

        LOG.info("Clone disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, response, LOG)) {
            return;
        }

        final var cloneDiskOperation = Operation.create(
            request.getUserId(),
            "CloneDisk: " + request.getDiskId(),
            Duration.ofDays(1),
            idempotencyKey,
            DiskServiceApi.CloneDiskMetadata.newBuilder().build());

        try {
            var diskOperation = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationsDao.create(cloneDiskOperation, tx);

                    final Disk origDisk;
                    try {
                        origDisk = getDisk(request.getDiskId(), tx);
                    } catch (StatusException e) {
                        LOG.error(e.getStatus().getDescription());
                        operationsDao.fail(cloneDiskOperation.id(), toProto(e.getStatus()), tx);
                        tx.commit();
                        metrics.cloneDiskError.inc();
                        return null;
                    }

                    if (origDisk == null) {
                        var status = Status.NOT_FOUND.withDescription(
                            "Unable to clone disk [operation_id=%s]. Disk %s not found."
                                .formatted(cloneDiskOperation.id(), request.getDiskId()));
                        LOG.error(status.getDescription());
                        operationsDao.fail(cloneDiskOperation.id(), toProto(status), tx);
                        tx.commit();
                        metrics.cloneDiskError.inc();
                        return null;
                    }

                    var ctx = DiskManager.OuterOperation.from(cloneDiskOperation);

                    final DiskOperation action;
                    try {
                        action = diskManager.newCloneDiskOperation(
                            ctx, origDisk, DiskSpec.fromProto(request.getNewDiskSpec()),
                            new DiskMeta(request.getUserId()));
                    } catch (IllegalArgumentException e) {
                        var status = Status.INVALID_ARGUMENT.withDescription(
                            "Unable to clone disk [operation_id=%s]. %s"
                                .formatted(cloneDiskOperation.id(), e.getMessage()));
                        LOG.error(status.getDescription());
                        operationsDao.fail(cloneDiskOperation.id(), toProto(status), tx);
                        tx.commit();
                        metrics.cloneDiskError.inc();
                        return null;
                    }

                    tx.commit();
                    metrics.cloneDiskStart.inc();

                    return action;
                }
            });

            if (diskOperation != null) {
                InjectedFailures.failCloneDisk0();
                executor.startNew(diskOperation.deferredAction());
            }
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationsDao, response, LOG))
            {
                return;
            }

            LOG.error("Cannot create CloneDisk for owner {}: {}", request.getUserId(), e.getMessage(), e);
            metrics.cloneDiskError.inc();
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        response.onNext(cloneDiskOperation.toProto());
        response.onCompleted();
    }

    @Override
    public void deleteDisk(DeleteDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("Delete disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, response, LOG)) {
            return;
        }

        final var deleteDiskOperation = Operation.create(
            "internal",
            "DeleteDisk: " + request.getDiskId(),
            Duration.ofDays(1),
            idempotencyKey,
            DiskServiceApi.DeleteDiskMetadata.newBuilder().build());

        try {
            var diskOperation = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationsDao.create(deleteDiskOperation, tx);

                    final Disk disk;
                    try {
                        disk = getDisk(request.getDiskId(), tx);
                    } catch (StatusException e) {
                        LOG.error(e.getStatus().getDescription());
                        operationsDao.fail(deleteDiskOperation.id(), toProto(e.getStatus()), tx);
                        tx.commit();
                        metrics.deleteDiskError.inc();
                        return null;
                    }

                    if (disk == null) {
                        var status = Status.NOT_FOUND.withDescription(
                            "Unable to delete disk [operation_id=%s]. Disk %s not found."
                                .formatted(deleteDiskOperation.id(), request.getDiskId()));
                        LOG.error(status.getDescription());
                        operationsDao.fail(deleteDiskOperation.id(), toProto(status), tx);
                        tx.commit();
                        metrics.deleteDiskError.inc();
                        return null;
                    }

                    var ctx = DiskManager.OuterOperation.from(deleteDiskOperation);
                    var diskOp = diskManager.newDeleteDiskOperation(ctx, request.getDiskId());
                    diskOpDao.createDiskOp(diskOp, tx);

                    tx.commit();
                    metrics.deleteDiskStart.inc();
                    return diskOp;
                }
            });

            if (diskOperation != null) {
                InjectedFailures.failDeleteDisk0();
                executor.startNew(diskOperation.deferredAction());
            }
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationsDao, response, LOG))
            {
                return;
            }

            LOG.error("Cannot delete disk {}: {}", request.getDiskId(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            metrics.deleteDiskError.inc();
            return;
        }

        response.onNext(deleteDiskOperation.toProto());
        response.onCompleted();
    }

    @Nullable
    private Disk getDisk(String diskId, @Nullable TransactionHandle transaction) throws SQLException, StatusException {
        final Disk dbDisk = diskDao.get(diskId, transaction);
        final Disk disk = diskManager.get(diskId);
        if (dbDisk != null && !dbDisk.equals(disk)) {
            LOG.error("The disk with id={} is in DB but already deleted/modified from manager (e.g. YCloud)", diskId);
            throw Status.DATA_LOSS
                .withDescription("Disk id=" + diskId + " has been modified outside of disk service")
                .asException();
        }
        return dbDisk != null ? disk : null;
    }

    private static boolean validateRequest(CreateDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        if (request.getUserId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("user_id not set").asException());
            return false;
        }
        switch (request.getDiskDescriptionCase()) {
            case DISK_SPEC -> {
                if (!validateDiskSpec(request.getDiskSpec(), response)) {
                    return false;
                }
            }
            case EXISTING_DISK -> {
                if (request.getExistingDisk().getDiskId().isBlank()) {
                    response.onError(Status.INVALID_ARGUMENT
                        .withDescription("existing_disk.disk_id not set").asException());
                    return false;
                }
            }
            case DISKDESCRIPTION_NOT_SET -> {
                response.onError(Status.INVALID_ARGUMENT.withDescription("disk_description not set").asException());
                return false;
            }
        }
        return true;
    }

    private static boolean validateRequest(CloneDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        if (request.getUserId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("user_id not set").asException());
            return false;
        }
        if (request.getDiskId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("disk_id not set").asException());
            return false;
        }
        return validateDiskSpec(request.getNewDiskSpec(), response);
    }

    private static boolean validateDiskSpec(DiskApi.DiskSpec spec, StreamObserver<LongRunning.Operation> response) {
        if (spec.getName().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("disk_spec.name not set").asException());
            return false;
        }
        if (spec.getZoneId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("disk_spec.zone_id not set").asException());
            return false;
        }
        if (spec.getSizeGb() < 1) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("disk_spec.size_gb not set").asException());
            return false;
        }
        if (spec.getType() == DiskApi.DiskType.DISK_TYPE_UNSPECIFIED) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("disk_spec.type not set").asException());
            return false;
        }
        return true;
    }
}
