package ai.lzy.allocator.disk;

import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.DiskServiceApi;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Objects;
import javax.inject.Named;
import javax.inject.Singleton;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Singleton
public class DiskService extends DiskServiceGrpc.DiskServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(DiskService.class);

    private final DiskManager diskManager;
    private final OperationDao operationsDao;
    private final DiskStorage diskStorage;
    private final AllocatorDataSource storage;
    private final Metrics metrics = new Metrics();

    public DiskService(DiskManager diskManager, DiskStorage diskStorage, AllocatorDataSource storage,
                       @Named("AllocatorOperationDao") OperationDao operationDao)
    {
        this.diskManager = diskManager;
        this.diskStorage = diskStorage;
        this.storage = storage;
        this.operationsDao = operationDao;
    }

    @Override
    public void createDisk(DiskServiceApi.CreateDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("Create disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, response, LOG)) {
            return;
        }

        final Operation createDiskOperation = Operation.create(
            request.getUserId(),
            "CreateDisk",
            idempotencyKey,
            DiskServiceApi.CreateDiskMetadata.getDefaultInstance());

        try {
            withRetries(LOG, () -> operationsDao.create(createDiskOperation, null));
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationsDao, response, LOG))
            {
                return;
            }

            LOG.error("Cannot create \"create_disk_operation\" for owner {}: {}",
                request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }
        response.onNext(createDiskOperation.toProto());
        response.onCompleted();

        // TODO: don't occupy grpc-thread
        try {
            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    final Disk disk;
                    if (request.hasDiskSpec()) {
                        final Histogram.Timer timer = metrics.createNewDiskDuration.startTimer();
                        disk = diskManager.create(
                            DiskSpec.fromProto(request.getDiskSpec()),
                            new DiskMeta(request.getUserId())
                        );
                        timer.close();
                        diskStorage.insert(disk, tx);
                        metrics.createDiskNew.inc();
                    } else if (request.hasExistingDisk()) {
                        final String diskId = request.getExistingDisk().getDiskId();
                        disk = diskManager.get(diskId);
                        if (disk == null) {
                            var excMessage = "Disk with id " + diskId + " not found";

                            var status = com.google.rpc.Status.newBuilder()
                                .setCode(Status.NOT_FOUND.getCode().value())
                                .setMessage(excMessage)
                                .build();

                            operationsDao.updateError(createDiskOperation.id(), status.toByteArray(), tx);

                            tx.commit();
                            metrics.createDiskError.inc();
                            return;
                        }
                        diskStorage.insert(disk, tx);
                        metrics.createDiskExisting.inc();
                    } else {
                        var excMessage = "Unknown disk spec type " + request.getDiskDescriptionCase();

                        var status = com.google.rpc.Status.newBuilder()
                            .setCode(Status.INVALID_ARGUMENT.getCode().value())
                            .setMessage(excMessage)
                            .build();

                        operationsDao.updateError(createDiskOperation.id(), status.toByteArray(), tx);

                        tx.commit();
                        metrics.createDiskError.inc();
                        return;
                    }

                    var metaBytes = Any.pack(
                            DiskServiceApi.CreateDiskMetadata.newBuilder()
                                .setDiskId(disk.id())
                                .build())
                        .toByteArray();

                    var responseBytes = Any.pack(
                            DiskServiceApi.CreateDiskResponse.newBuilder()
                                .setDisk(disk.toProto())
                                .build())
                        .toByteArray();

                    operationsDao.updateMetaAndResponse(createDiskOperation.id(), metaBytes, responseBytes, tx);

                    tx.commit();
                }
            });
        } catch (Exception e) {
            var errorMessage = "Error while executing transaction for create disk operation_id=%s: %s"
                .formatted(createDiskOperation.id(), e.getMessage());
            LOG.error(errorMessage, e);
            metrics.createDiskError.inc();

            OperationDao.failOperation(operationsDao, createDiskOperation.id(),
                toProto(Status.INTERNAL.withDescription(errorMessage)), LOG);
        }
    }

    @Override
    public void cloneDisk(DiskServiceApi.CloneDiskRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("Clone disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationsDao, idempotencyKey, response, LOG)) {
            return;
        }

        final Operation cloneDiskOperation = Operation.create(
            request.getUserId(),
            "CloneDisk",
            idempotencyKey,
            DiskServiceApi.CloneDiskMetadata.getDefaultInstance());

        try {
            withRetries(LOG, () -> operationsDao.create(cloneDiskOperation, null));
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationsDao, response, LOG))
            {
                return;
            }

            LOG.error("Cannot create \"clone_disk_operation\" for owner {}: {}",
                request.getUserId(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }
        response.onNext(cloneDiskOperation.toProto());
        response.onCompleted();

        // TODO: don't occupy grpc-thread
        try {
            withRetries(LOG,
                () -> {
                    try (var transaction = TransactionHandle.create(storage)) {
                        try {
                            final String existingDiskId = request.getDiskId();
                            final Disk existingDisk = getDisk(existingDiskId, transaction);

                            final Disk clonedDisk = diskManager.clone(
                                existingDisk, DiskSpec.fromProto(request.getNewDiskSpec()),
                                new DiskMeta(request.getUserId()));

                            diskStorage.insert(clonedDisk, transaction);

                            var metaBytes = Any.pack(DiskServiceApi.CloneDiskMetadata.newBuilder()
                                .setDiskId(clonedDisk.id()).build()).toByteArray();
                            var responseBytes = Any.pack(DiskServiceApi.CloneDiskResponse.newBuilder()
                                .setDisk(clonedDisk.toProto()).build()).toByteArray();

                            operationsDao.updateMetaAndResponse(cloneDiskOperation.id(), metaBytes, responseBytes,
                                transaction);

                            metrics.cloneDisk.inc();
                        } catch (NotFoundException | IllegalArgumentException e) {
                            var status = com.google.rpc.Status.newBuilder()
                                .setCode(Status.INVALID_ARGUMENT.getCode().value())
                                .setMessage("Unable to clone disk [operation_id=" + cloneDiskOperation.id() + "] "
                                    + " Invalid argument; cause=" + e.getMessage())
                                .build();
                            operationsDao.updateError(cloneDiskOperation.id(), status.toByteArray(), transaction);
                        } catch (StatusException e) {
                            var status = com.google.rpc.Status.newBuilder()
                                .setCode(e.getStatus().getCode().value())
                                .setMessage(Objects.toString(e.getStatus().getDescription(), ""))
                                .build();
                            operationsDao.updateError(cloneDiskOperation.id(), status.toByteArray(), transaction);
                        }

                        transaction.commit();
                    }
                    return (Void) null;
                }
            );
        } catch (Exception e) {
            var errorMessage = "Error while executing transaction for clone disk operation_id=%s: %s"
                .formatted(cloneDiskOperation.id(), e.getMessage());
            LOG.error(errorMessage, e);

            OperationDao.failOperation(operationsDao, cloneDiskOperation.id(),
                toProto(Status.INTERNAL.withDescription(errorMessage)), LOG);

            metrics.cloneDiskError.inc();
        }
    }

    @Override
    public void deleteDisk(DiskServiceApi.DeleteDiskRequest request,
                           StreamObserver<DiskServiceApi.DeleteDiskResponse> responseObserver)
    {
        LOG.info("Delete disk request {}", ProtoPrinter.safePrinter().shortDebugString(request));
        final String diskId = request.getDiskId();
        try {
            withRetries(LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    getDisk(diskId, transaction);
                    responseObserver.onNext(DiskServiceApi.DeleteDiskResponse.getDefaultInstance());
                    responseObserver.onCompleted();

                    diskStorage.remove(diskId, transaction);
                    diskManager.delete(diskId);
                    transaction.commit();
                } catch (NotFoundException e) {
                    LOG.error("Disk with id={} not found for deletion exception={}", diskId, e.getMessage(), e);
                    responseObserver.onError(Status.NOT_FOUND.asException());
                } catch (StatusException e) {
                    responseObserver.onError(e);
                }
                return null;
            });
        } catch (Exception e) {
            LOG.error("Error while executing transaction for delete disk id={}: {}", diskId, e.getMessage(), e);
            metrics.deleteDiskError.inc();
            responseObserver.onError(Status.INTERNAL.withDescription("Error while deleting disk").asException());
        }
    }

    private Disk getDisk(String diskId, TransactionHandle transaction)
        throws SQLException, NotFoundException, StatusException
    {
        final Disk dbDisk = diskStorage.get(diskId, transaction);
        final Disk disk = diskManager.get(diskId);
        if (dbDisk != null && !dbDisk.equals(disk)) {
            LOG.error("The disk with id={} is in DB but already deleted/modified from manager (e.g. YCloud)", diskId);
            throw Status.DATA_LOSS
                .withDescription("Disk id=" + diskId + " has been modified outside of disk service")
                .asException();
        }
        if (dbDisk == null) {
            throw new NotFoundException("Disk with id=" + diskId + " not found");
        }
        return disk;
    }

    private static final class Metrics {
        private final Counter createDiskExisting = Counter
            .build("create_disk_existing", "Create disk from existing cloud disk")
            .subsystem("allocator")
            .register();

        private final Counter createDiskNew = Counter
            .build("create_disk_new", "Create new disk")
            .subsystem("allocator")
            .register();

        private final Counter cloneDisk = Counter
            .build("clone_disk", "Clone disk")
            .subsystem("allocator")
            .register();

        private final Counter createDiskError = Counter
            .build("create_disk_error", "Disk creation errors")
            .subsystem("allocator")
            .register();

        private final Counter cloneDiskError = Counter
            .build("clone_disk_error", "Disk clone errors")
            .subsystem("allocator")
            .register();

        private final Counter deleteDiskError = Counter
            .build("delete_disk_error", "Disk deletion errors")
            .subsystem("allocator")
            .register();

        private final Histogram createNewDiskDuration = Histogram
            .build("create_new_disk_duration", "Create new disk duration (sec)")
            .subsystem("allocator")
            .buckets(0.001, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0)
            .register();
    }
}
