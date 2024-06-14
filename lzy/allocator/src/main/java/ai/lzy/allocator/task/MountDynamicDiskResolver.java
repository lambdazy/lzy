package ai.lzy.allocator.task;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.allocator.alloc.MountDynamicDiskAction;
import ai.lzy.allocator.alloc.dao.DynamicMountDao;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.longrunning.task.OperationTask;
import ai.lzy.longrunning.task.OperationTaskScheduler;
import ai.lzy.longrunning.task.ResolverUtils;
import ai.lzy.longrunning.task.TypedOperationTaskResolver;
import ai.lzy.longrunning.task.dao.OperationTaskDao;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.time.Duration;

@Singleton
public class MountDynamicDiskResolver implements TypedOperationTaskResolver {
    private static final Logger LOG = LogManager.getLogger(MountDynamicDiskResolver.class);

    private static final String TYPE = "MOUNT";
    public static final String VM_ID_FIELD = "vm_id";
    public static final String DYNAMIC_MOUNT_ID_FIELD = "dynamic_mount_id";

    private final VmDao vmDao;
    private final DynamicMountDao dynamicMountDao;
    private final AllocationContext allocationContext;
    private final OperationTaskDao operationTaskDao;
    private final OperationTaskScheduler taskScheduler; //todo circular dependency
    private final Duration leaseDuration;

    public MountDynamicDiskResolver(VmDao vmDao, DynamicMountDao dynamicMountDao, AllocationContext allocationContext,
                                    OperationTaskDao operationTaskDao, OperationTaskScheduler taskScheduler,
                                    Duration leaseDuration)
    //todo mark duration with a qualifier
    {
        this.vmDao = vmDao;
        this.dynamicMountDao = dynamicMountDao;
        this.allocationContext = allocationContext;
        this.operationTaskDao = operationTaskDao;
        this.taskScheduler = taskScheduler;
        this.leaseDuration = leaseDuration;
    }

    @Override
    public Result resolve(OperationTask opTask, @Nullable TransactionHandle tx) throws SQLException {
        var vmId = ResolverUtils.readString(opTask.metadata(), VM_ID_FIELD);
        if (vmId == null) {
            LOG.error("{} field is not present in task {} metadata", VM_ID_FIELD, opTask.id());
            return Result.BAD_STATE;
        }
        var dynamicMountId = ResolverUtils.readString(opTask.metadata(), DYNAMIC_MOUNT_ID_FIELD);
        if (dynamicMountId == null) {
            LOG.error("{} field is not present in task {} metadata", DYNAMIC_MOUNT_ID_FIELD, opTask.id());
            return Result.BAD_STATE;
        }
        var vm = vmDao.get(vmId, tx);
        if (vm == null) {
            LOG.error("VM {} is not present for task", vmId);
            return Result.STALE;
        } else if (vm.status() != Vm.Status.RUNNING) {
            LOG.error("VM {} is in wrong status: {}", vmId, vm.status());
            return Result.STALE;
        }
        var dynamicMount = dynamicMountDao.get(dynamicMountId, false, tx);
        if (dynamicMount == null) {
            LOG.error("Dynamic mount {} is not present for task", dynamicMountId);
            return Result.STALE;
        } else if (dynamicMount.state() != DynamicMount.State.PENDING) {
            LOG.error("Dynamic mount {} is in wrong status: {}", dynamicMount.id(), dynamicMount.state());
            return Result.STALE;
        }
        return Result.success(new MountDynamicDiskAction(vm, dynamicMount, allocationContext, opTask, operationTaskDao,
            leaseDuration, taskScheduler));
    }

    @Override
    public String type() {
        return TYPE;
    }

}
