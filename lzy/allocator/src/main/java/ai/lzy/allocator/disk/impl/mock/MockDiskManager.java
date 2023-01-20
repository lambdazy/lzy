package ai.lzy.allocator.disk.impl.mock;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskOperation;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.DiskServiceApi;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import lombok.Lombok;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

@Requires(property = "allocator.yc-credentials.enabled", value = "false")
@Singleton
public class MockDiskManager implements DiskManager {
    private static final Logger LOG = LogManager.getLogger(MockDiskManager.class);

    private final String instanceId;
    private final AllocatorDataSource storage;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final OperationDao operationsDao;
    private final Map<String, Disk> disks = new ConcurrentHashMap<>();

    @Inject
    public MockDiskManager(ServiceConfig config, AllocatorDataSource storage, DiskDao diskDao, DiskOpDao diskOpDao,
                           @Named("AllocatorOperationDao") OperationDao operationsDao)
    {
        this.instanceId = config.getInstanceId();
        this.storage = storage;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.operationsDao = operationsDao;
    }

    public void put(Disk disk) {
        disks.put(disk.id(), disk);
    }

    public void delete(String diskId) throws NotFoundException {
        var disk = disks.remove(diskId);
        if (disk == null) {
            throw new NotFoundException();
        }
    }

    @Nullable
    @Override
    public Disk get(String id) {
        return disks.get(id);
    }

    @Override
    public DiskOperation newCreateDiskOperation(OuterOperation outerOp, DiskSpec spec, DiskMeta meta) {
        final String id = UUID.randomUUID().toString();
        final Disk disk = new Disk(id, spec, meta);
        disks.put(id, disk);

        LOG.info("MockCreateDisk succeeded, created disk {}", disk);

        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.CREATE,
            "",
            () -> {
                var m = Any.pack(
                        DiskServiceApi.CreateDiskMetadata.newBuilder()
                            .setDiskId(id)
                            .build());

                var r = Any.pack(
                        DiskServiceApi.CreateDiskResponse.newBuilder()
                            .setDisk(disk.toProto())
                            .build());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskDao.insert(disk, tx);
                            diskOpDao.deleteDiskOp(outerOp.opId(), tx);
                            operationsDao.complete(outerOp.opId(), m, r, tx);
                            tx.commit();
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Override
    public DiskOperation newCloneDiskOperation(OuterOperation outerOp, Disk disk, DiskSpec newDiskSpec,
                                               DiskMeta newDiskMeta)
    {
        final var notFound = !disks.containsKey(disk.id());

        if (disk.spec().sizeGb() > newDiskSpec.sizeGb()) {
            throw new IllegalArgumentException("Cannot decrease size during clone");
        }

        final Disk newDisk;
        if (notFound) {
            newDisk = null;
        } else {
            final String id = UUID.randomUUID().toString();
            newDisk = new Disk(id, newDiskSpec, newDiskMeta);
            disks.put(id, newDisk);
        }

        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.CLONE,
            "",
            () -> {
                if (notFound) {
                    var status = Status.NOT_FOUND.withDescription("Disk not found");
                    try {
                        operationsDao.fail(outerOp.opId(), toProto(status), null);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                }


                var m = Any.pack(
                        DiskServiceApi.CloneDiskMetadata.newBuilder()
                            .setDiskId(newDisk.id())
                            .build());

                var r = Any.pack(
                        DiskServiceApi.CloneDiskResponse.newBuilder()
                            .setDisk(newDisk.toProto())
                            .build());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskOpDao.deleteDiskOp(outerOp.opId(), tx);
                            diskDao.insert(newDisk, tx);
                            operationsDao.complete(outerOp.opId(), m, r, tx);
                            tx.commit();
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Override
    public DiskOperation newDeleteDiskOperation(OuterOperation outerOp, String diskId) {
        if (!disks.containsKey(diskId)) {
            throw Lombok.sneakyThrow(new NotFoundException());
        }

        disks.remove(diskId);

        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.DELETE,
            "",
            () -> {
                var m = Any.pack(DiskServiceApi.DeleteDiskMetadata.newBuilder().build());
                var r = Any.pack(DiskServiceApi.DeleteDiskResponse.newBuilder().build());

                try {
                    withRetries(LOG, () -> {
                        try (var tx = TransactionHandle.create(storage)) {
                            diskOpDao.deleteDiskOp(outerOp.opId(), tx);
                            diskDao.remove(diskId, tx);
                            operationsDao.complete(outerOp.opId(), m, r, tx);
                            tx.commit();
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Override
    public DiskOperation restoreDiskOperation(DiskOperation template) {
        // TODO:
        return template;
    }
}
