package ai.lzy.allocator.disk;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.model.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.allocator.DiskServiceApi;
import ai.lzy.v1.allocator.DiskServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.sql.SQLException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
                           StreamObserver<OperationService.Operation> responseObserver)
    {
        LOG.info("Create disk request {}", JsonUtils.printRequest(request));

        final Operation[] createDiskOperation = new Operation[1];
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> operations.create("Create disk", request.getUserId(), null, null),
            op -> createDiskOperation[0] = op,
            e -> {
                LOG.error("Cannot create \"create_disk_operation\" for owner {}: {}",
                    request.getUserId(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        );
        if (createDiskOperation[0] == null) {
            return;
        }
        responseObserver.onNext(createDiskOperation[0].toProto());
        responseObserver.onCompleted();

        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    final Disk disk;
                    try {
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
                                createDiskOperation[0].setError(Status.NOT_FOUND.withDescription(excMessage));
                                operations.update(createDiskOperation[0], transaction);
                                transaction.commit();
                                metrics.createDiskError.inc();
                                return null;
                            }
                            diskStorage.insert(disk, transaction);
                            metrics.createDiskExisting.inc();
                        } else {
                            final String excMessage = "Unknown disk spec type " + request.getDiskDescriptionCase();
                            createDiskOperation[0].setError(Status.INVALID_ARGUMENT.withDescription(excMessage));
                            operations.update(createDiskOperation[0], transaction);
                            transaction.commit();
                            metrics.createDiskError.inc();
                            return null;
                        }
                    } catch (RuntimeException e) {
                        createDiskOperation[0].setError(Status.INTERNAL.withDescription(e.getMessage()));
                        operations.update(createDiskOperation[0], transaction);
                        transaction.commit();
                        metrics.createDiskError.inc();
                        return null;
                    }
                    createDiskOperation[0].modifyMeta(Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(disk.id())
                            .build()
                    ));
                    createDiskOperation[0].setResponse(Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build()
                    ));
                    operations.update(createDiskOperation[0], transaction);
                    transaction.commit();
                }
                return (Void) null;
            },
            ok -> {},
            ex -> LOG.error("Error while executing transaction for create disk operation_id={}: {}",
                createDiskOperation[0].id(), ex.getMessage(), ex)
        );
    }

    @Override
    public void cloneDisk(DiskServiceApi.CloneDiskRequest request,
                          StreamObserver<OperationService.Operation> responseObserver)
    {
        LOG.info("Clone disk request {}", JsonUtils.printRequest(request));

        final Operation[] cloneDiskOperation = new Operation[1];
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> operations.create("Clone disk", request.getUserId(), null, null),
            op -> cloneDiskOperation[0] = op,
            e -> {
                LOG.error("Cannot create \"clone_disk_operation\" for owner {}: {}",
                    request.getUserId(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        );
        if (cloneDiskOperation[0] == null) {
            return;
        }
        responseObserver.onNext(cloneDiskOperation[0].toProto());
        responseObserver.onCompleted();

        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    final String existingDiskId = request.getDiskId();
                    final Disk existingDisk;
                    try {
                        existingDisk = getDisk(existingDiskId, transaction);
                    } catch (StatusException e) {
                        cloneDiskOperation[0].setError(e.getStatus());
                        operations.update(cloneDiskOperation[0], transaction);
                        transaction.commit();
                        return null;
                    }

                    try {
                        final Disk clonedDisk = diskManager.clone(
                            existingDisk, DiskSpec.fromProto(request.getNewDiskSpec()),
                            new DiskMeta(request.getUserId()));
                        diskStorage.insert(clonedDisk, transaction);
                        cloneDiskOperation[0].modifyMeta(Any.pack(DiskServiceApi.CloneDiskMetadata.newBuilder()
                            .setDiskId(clonedDisk.id()).build()));
                        cloneDiskOperation[0].setResponse(Any.pack(DiskServiceApi.CloneDiskResponse.newBuilder()
                            .setDisk(clonedDisk.toProto()).build()));
                        metrics.cloneDisk.inc();
                    } catch (Exception e) {
                        cloneDiskOperation[0].setError(Status.INTERNAL.withDescription(
                            "CloneDisk operation with id " + cloneDiskOperation[0].id()
                            + " has failed with exception=" + e.getMessage()
                        ));
                        metrics.cloneDiskError.inc();
                    }
                    operations.update(cloneDiskOperation[0], transaction);
                    transaction.commit();
                }
                return (Void) null;
            },
            ok -> {},
            ex -> LOG.error("Error while executing transaction for clone disk operation_id={}: {}",
                cloneDiskOperation[0].id(), ex.getMessage(), ex)
        );
    }

    @Override
    public void deleteDisk(DiskServiceApi.DeleteDiskRequest request,
                           StreamObserver<DiskServiceApi.DeleteDiskResponse> responseObserver)
    {
        LOG.info("Delete disk request {}", JsonUtils.printRequest(request));
        final String diskId = request.getDiskId();
        withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> {
                try (var transaction = TransactionHandle.create(storage)) {
                    try {
                        getDisk(diskId, transaction);
                    } catch (StatusException e) {
                        responseObserver.onError(e);
                        return null;
                    }
                    responseObserver.onNext(DiskServiceApi.DeleteDiskResponse.getDefaultInstance());
                    responseObserver.onCompleted();

                    try {
                        diskStorage.remove(diskId, transaction);
                        diskManager.delete(diskId);
                    } catch (Exception e) {
                        LOG.error(
                            "Disk id={} deletion has failed with exception={}", diskId, e.getMessage(), e
                        );
                        metrics.deleteDiskError.inc();
                    }
                    transaction.commit();
                }
                return (Void) null;
            },
            ok -> {},
            ex -> LOG.error("Error while executing transaction for delete disk id={}: {}",
                diskId, ex.getMessage(), ex)
        );
    }

    private Disk getDisk(String diskId, TransactionHandle transaction) throws SQLException, StatusException {
        final Disk dbDisk = diskStorage.get(diskId, transaction);
        final Disk disk = diskManager.get(diskId);
        if (dbDisk != null && !dbDisk.equals(disk)) {
            LOG.error(
                "The disk with id={} is in DB but already deleted/modified from manager (e.g. YCloud)",
                diskId
            );
            throw Status.INTERNAL
                .withDescription("Disk id=" + diskId + " has been modified outside of disk service")
                .asException();
        }
        if (dbDisk == null) {
            throw Status.NOT_FOUND.withDescription("Disk with id=" + diskId + " not found").asException();
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
