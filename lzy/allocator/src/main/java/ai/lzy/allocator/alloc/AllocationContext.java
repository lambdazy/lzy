package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.MountHolderManager;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.allocator.volume.dao.VolumeClaimDao;
import ai.lzy.allocator.volume.dao.VolumeDao;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public record AllocationContext(
    AllocatorDataSource storage,
    @Named("AllocatorOperationDao") OperationDao operationsDao,
    VmDao vmDao,
    @Named("AllocatorOperationsExecutor") OperationsExecutor executor,
    @Named("AllocatorSubjectServiceClient") SubjectServiceGrpcClient subjectClient,
    VmAllocator allocator,
    TunnelAllocator tunnelAllocator,
    AllocatorMetrics metrics,
    @Named("AllocatorSelfWorkerId") String selfWorkerId,
    MountHolderManager mountHolderManager,
    VolumeManager volumeManager,
    DynamicMountDao dynamicMountDao,
    VolumeDao volumeDao,
    VolumeClaimDao volumeClaimDao,
    ServiceConfig.MountConfig mountConfig
)
{
    public void startNew(Runnable action) {
        executor.startNew(action);
    }

    public String startDeleteVmAction(Vm vm, String description, String reqid, Logger log) throws Exception {
        log.info("About to delete VM {}: {}", vm.vmId(), description);
        var action = createDeleteVmAction(vm, description, reqid, log);
        startNew(action);

        switch (vm.status()) {
            case RUNNING -> {
                metrics.runningVms.labels(vm.poolLabel()).dec();
            }
            case IDLE -> {
                metrics.cachedVms.labels(vm.poolLabel()).dec();
                metrics.cachedVmsTime.labels(vm.poolLabel())
                    .inc(Duration.between(vm.idleState().idleSice(), Instant.now()).getSeconds());
            }
        }

        return action.id();
    }

    public DeleteVmAction createDeleteVmAction(Vm vm, String description, String reqid, Logger log) throws Exception {
        return withRetries(log, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                var action = createDeleteVmAction(vm, description, reqid, tx);
                tx.commit();
                return action;
            }
        });
    }

    public DeleteVmAction createDeleteVmAction(Vm vm, String description, String reqid, @Nullable TransactionHandle tx)
        throws SQLException
    {
        var deleteOp = Operation.create(
            "system",
            description,
            Duration.ofDays(10),
            new Operation.IdempotencyKey("Delete VM " + vm.vmId(), vm.vmId()),
            /* meta */ null);

        var deleteState = new Vm.DeletingState(deleteOp.id(), selfWorkerId, reqid);

        operationsDao.create(deleteOp, tx);
        vmDao.delete(vm.vmId(), deleteState, tx);

        return new DeleteVmAction(vm, deleteOp.id(), this);
    }

    public UnmountDynamicDiskAction createUnmountAction(Vm vm, DynamicMount dynamicMount,
                                                        @Nullable TransactionHandle tx) throws SQLException
    {
        var op = Operation.create(
            "system",
            "Unmount mount %s from vm %s".formatted(dynamicMount.id(), vm.vmId()),
            Duration.ofDays(10),
            new Operation.IdempotencyKey("unmount-disk-%s-%s".formatted(vm.vmId(), dynamicMount.id()),
                vm.vmId() + dynamicMount.id()),
            null
        );
        operationsDao().create(op, tx);
        var update = DynamicMount.Update.builder()
            .state(DynamicMount.State.DELETING)
            .unmountOperationId(op.id())
            .build();

        dynamicMountDao().update(dynamicMount.id(), update, tx);
        var updatedMount = dynamicMount.apply(update);

        return new UnmountDynamicDiskAction(op.id(), vm, updatedMount, this);
    }
}
