package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.alloc.impl.kuber.TunnelAllocator;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.storage.AllocatorDataSource;
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
    @Named("AllocatorSelfWorkerId") String selfWorkerId
)
{
    public void submit(Runnable action) {
        executor.submit(action);
    }

    public String submitDeleteVmAction(Vm vm, String description, Logger log) throws Exception {
        log.info("About to delete VM {}: {}", vm.vmId(), description);
        var action = createDeleteVmAction(vm, description, log);
        submit(action);

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

    public DeleteVmAction createDeleteVmAction(Vm vm, String description, Logger log) throws Exception {
        return withRetries(log, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                var action = createDeleteVmAction(vm, description, tx);
                tx.commit();
                return action;
            }
        });
    }

    public DeleteVmAction createDeleteVmAction(Vm vm, String description, @Nullable TransactionHandle tx)
        throws SQLException
    {
        var deleteOp = Operation.create(
            "system",
            description,
            Duration.ofDays(10),
            new Operation.IdempotencyKey("Delete VM " + vm.vmId(), vm.vmId()),
            /* meta */ null);

        var deleteState = new Vm.DeletingState(deleteOp.id(), selfWorkerId);

        operationsDao.create(deleteOp, tx);
        vmDao.delete(vm.vmId(), deleteState, tx);

        return new DeleteVmAction(vm, deleteOp.id(), this);
    }
}
