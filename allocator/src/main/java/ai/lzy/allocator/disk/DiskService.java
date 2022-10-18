package ai.lzy.allocator.disk;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.JsonUtils;
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
import javax.inject.Inject;
import javax.inject.Singleton;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class DiskService extends DiskServiceGrpc.DiskServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(DiskService.class);

    private final DiskManager diskManager;
    private final OperationDao operations;
    private final DiskStorage diskStorage;
    private final AllocatorDataSource storage;
    private final Metrics metrics = new Metrics();

    @Inject
    public DiskService(DiskManager diskManager, OperationDao operations, DiskStorage diskStorage,
                       AllocatorDataSource storage)
    {
        this.diskManager = diskManager;
        this.operations = operations;
        this.diskStorage = diskStorage;
        this.storage = storage;
    }

    @Override
    public void createDisk(DiskServiceApi.CreateDiskRequest request,
                           StreamObserver<LongRunning.Operation> responseObserver)
    {
        LOG.info("Create disk request {}", JsonUtils.printRequest(request));

        final Operation createDiskOperation;
        try {
            createDiskOperation = withRetries(LOG,
                () -> operations.create(
                    "Create disk",
                    request.getUserId(),
                    Any.pack(DiskServiceApi.CreateDiskMetadata.getDefaultInstance()),
                    null)
            );
        } catch (Exception e) {
            LOG.error("Cannot create \"create_disk_operation\" for owner {}: {}",
                request.getUserId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }
        responseObserver.onNext(createDiskOperation.toProto());
        responseObserver.onCompleted();

        try {
            withRetries(LOG, () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    final Disk disk;
                    if (request.hasDiskSpec()) {
                        final Histogram.Timer timer = metrics.createNewDiskDuration.startTimer();
                        disk = diskManager.create(
                            DiskSpec.fromProto(request.getDiskSpec()),
                            new DiskMeta(request.getUserId())
                        );
                        timer.close();
                        diskStorage.insert(disk, transaction);
                        metrics.createDiskNew.inc();
                    } else if (request.hasExistingDisk()) {
                        final String diskId = request.getExistingDisk().getDiskId();
                        disk = diskManager.get(diskId);
                        if (disk == null) {
                            final String excMessage = "Disk with id " + diskId + " not found";
                            createDiskOperation.setError(Status.NOT_FOUND.withDescription(excMessage));
                            operations.update(createDiskOperation, transaction);
                            transaction.commit();
                            metrics.createDiskError.inc();
                            return null;
                        }
                        diskStorage.insert(disk, transaction);
                        metrics.createDiskExisting.inc();
                    } else {
                        final String excMessage = "Unknown disk spec type " + request.getDiskDescriptionCase();
                        createDiskOperation.setError(Status.INVALID_ARGUMENT.withDescription(excMessage));
                        operations.update(createDiskOperation, transaction);
                        transaction.commit();
                        metrics.createDiskError.inc();
                        return null;
                    }
                    createDiskOperation.modifyMeta(Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(disk.id())
                            .build()
                    ));
                    createDiskOperation.setResponse(Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build()
                    ));
                    operations.update(createDiskOperation, transaction);
                    transaction.commit();
                }
                return (Void) null;
            });
        } catch (Exception e) {
            final String errorMessage = "Error while executing transaction for create disk operation_id=%s: %s"
                .formatted(createDiskOperation.id(), e.getMessage());
            LOG.error(errorMessage, e);
            metrics.createDiskError.inc();
            failOperation(createDiskOperation, errorMessage);
        }
    }

    @Override
    public void cloneDisk(DiskServiceApi.CloneDiskRequest request,
                          StreamObserver<LongRunning.Operation> responseObserver)
    {
        LOG.info("Clone disk request {}", JsonUtils.printRequest(request));

        final Operation cloneDiskOperation;
        try {
            cloneDiskOperation = withRetries(LOG,
                () -> operations.create(
                    "Clone disk",
                    request.getUserId(),
                    Any.pack(DiskServiceApi.CloneDiskMetadata.getDefaultInstance()),
                    null)
            );
        } catch (Exception e) {
            LOG.error("Cannot create \"clone_disk_operation\" for owner {}: {}",
                request.getUserId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }
        responseObserver.onNext(cloneDiskOperation.toProto());
        responseObserver.onCompleted();

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

                            cloneDiskOperation.modifyMeta(Any.pack(DiskServiceApi.CloneDiskMetadata.newBuilder()
                                .setDiskId(clonedDisk.id()).build()));
                            cloneDiskOperation.setResponse(Any.pack(DiskServiceApi.CloneDiskResponse.newBuilder()
                                .setDisk(clonedDisk.toProto()).build()));

                            metrics.cloneDisk.inc();
                        } catch (NotFoundException | IllegalArgumentException e) {
                            cloneDiskOperation.setError(Status.INVALID_ARGUMENT.withDescription(
                                "Unable to clone disk [operation_id=" + cloneDiskOperation.id() + "] "
                                    + " Invalid argument; cause=" + e.getMessage()
                            ));
                        } catch (StatusException e) {
                            cloneDiskOperation.setError(e.getStatus());
                        }
                        operations.update(cloneDiskOperation, transaction);
                        transaction.commit();
                    }
                    return (Void) null;
                }
            );
        } catch (Exception e) {
            final String errorMessage = "Error while executing transaction for clone disk operation_id=%s: %s"
                .formatted(cloneDiskOperation.id(), e.getMessage());
            LOG.error(errorMessage, e);
            failOperation(cloneDiskOperation, errorMessage);
            metrics.cloneDiskError.inc();
        }
    }

    @Override
    public void deleteDisk(DiskServiceApi.DeleteDiskRequest request,
                           StreamObserver<DiskServiceApi.DeleteDiskResponse> responseObserver)
    {
        LOG.info("Delete disk request {}", JsonUtils.printRequest(request));
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

    private void failOperation(Operation createDiskOperation, String errorMessage) {
        try {
            withRetries(LOG,
                () -> {
                    createDiskOperation.setError(Status.INTERNAL.withDescription(errorMessage));
                    operations.update(createDiskOperation, null);
                    return null;
                }
            );
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                createDiskOperation, errorMessage, ex.getMessage(), ex);
        }
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
