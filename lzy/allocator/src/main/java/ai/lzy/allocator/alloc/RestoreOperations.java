package ai.lzy.allocator.alloc;

import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.GrpcHeaders;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class RestoreOperations {

    private static final Logger LOG = LogManager.getLogger(RestoreOperations.class);

    private final AllocationContext allocationContext;

    public RestoreOperations(AllocationContext allocationContext) {
        this.allocationContext = allocationContext;
    }

    @PostConstruct
    public void restoreRunningActions() {
        restoreMetrics();
        restoreVmActions();
        restoreDeletingSessions();
        restoreMountDynamicDiskActions();
        restoreUnmountDynamicDiskActions();
    }

    private void restoreMetrics() {
        try {
            var vms = allocationContext.vmDao().loadRunningVms(allocationContext.selfWorkerId(), null);
            if (!vms.isEmpty()) {
                int run = 0;
                int idle = 0;
                for (var vm : vms) {
                    switch (vm.status()) {
                        case RUNNING -> {
                            allocationContext.metrics().runningVms.labels(vm.poolLabel()).inc();
                            run++;
                        }
                        case IDLE -> {
                            allocationContext.metrics().cachedVms.labels(vm.poolLabel()).inc();
                            idle++;
                        }
                        default -> throw new RuntimeException("Unexpected state: " + vm);
                    }
                }
                LOG.info("Found {} cached and {} running VMs on allocator {}",
                    idle, run, allocationContext.selfWorkerId());
            }
        } catch (SQLException e) {
            LOG.error("Failed to restore metrics", e);
            throw new RuntimeException(e);
        }
    }

    private void restoreVmActions() {
        try {
            var vms = allocationContext.vmDao().loadActiveVmsActions(allocationContext.selfWorkerId(), null);
            if (!vms.isEmpty()) {
                LOG.warn("Found {} not completed VM actions on allocator {}",
                    vms.size(), allocationContext.selfWorkerId());

                vms.forEach(vm -> {
                    var action = switch (vm.status()) {
                        case ALLOCATING -> GrpcHeaders.withContext()
                            .withHeader(GrpcHeaders.X_REQUEST_ID, vm.allocateState().reqid())
                            .run(() -> new AllocateVmAction(vm, allocationContext, true));
                        case DELETING -> {
                            var deleteOpId = vm.deleteState().operationId();
                            yield GrpcHeaders.withContext()
                                .withHeader(GrpcHeaders.X_REQUEST_ID, vm.deleteState().reqid())
                                .run(() -> new DeleteVmAction(vm, deleteOpId, allocationContext));
                        }
                        case IDLE, RUNNING -> throw new RuntimeException("Unexpected Vm state %s".formatted(vm));
                    };
                    allocationContext.startNew(action);
                });
            } else {
                LOG.info("Not completed allocations weren't found.");
            }
        } catch (SQLException e) {
            LOG.error("Failed to restore VM actions", e);
            throw new RuntimeException(e);
        }
    }

    private void restoreDeletingSessions() {
        try {
            var sessions = allocationContext.sessionDao().listDeleting(null);
            if (!sessions.isEmpty()) {
                LOG.info("Found {} not completed sessions removal", sessions.size());
                sessions.forEach(s -> {
                    var reqid = Optional.ofNullable(s.deleteReqid()).orElse("unknown");
                    GrpcHeaders.withContext()
                        .withHeader(GrpcHeaders.X_REQUEST_ID, reqid)
                        .run(() -> allocationContext.startNew(
                            new DeleteSessionAction(s, s.deleteOpId(), allocationContext,
                                allocationContext.sessionDao())));
                });
            }
        } catch (SQLException e) {
            LOG.error("Failed to restore sessions removal", e);
            throw new RuntimeException(e);
        }
    }

    private void restoreMountDynamicDiskActions() {
        LOG.info("Restoring mount dynamic disk actions");
        try {
            List<OperationRunnerBase> actions = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var pendingMounts = allocationContext.dynamicMountDao()
                        .getPending(allocationContext.selfWorkerId(), tx);
                    LOG.info("Found {} pending mounts", pendingMounts.size());
                    if (pendingMounts.isEmpty()) {
                        return List.of();
                    }
                    var mountsByVmId = pendingMounts.stream()
                        .collect(Collectors.groupingBy(DynamicMount::vmId));

                    var vms = allocationContext.vmDao().loadByIds(mountsByVmId.keySet(), tx);
                    Map<String, Vm> vmById = vms.stream().collect(Collectors.toMap(Vm::vmId, v -> v));
                    var actionsToRun = new ArrayList<OperationRunnerBase>(pendingMounts.size());
                    var mountsToRemove = new ArrayList<DynamicMount>();
                    mountsByVmId.forEach((vmId, mounts) -> {
                        var vm = vmById.get(vmId);
                        if (vm == null) {
                            LOG.warn("Vm {} not found for mounts {}", vmId, mounts);
                            mountsToRemove.addAll(mounts);
                            return;
                        }
                        for (var mount : mounts) {
                            var action = new MountDynamicDiskAction(vm, mount, allocationContext);
                            actionsToRun.add(action);
                        }
                    });

                    for (var dynamicMount : mountsToRemove) {
                        var unmountAction = allocationContext.createUnmountAction(null, dynamicMount, tx);
                        actionsToRun.add(unmountAction.getLeft());
                    }
                    tx.commit();
                    LOG.info("Found {} mount dynamic disk actions to restore, {} of them are for unmount",
                        actionsToRun.size(), mountsToRemove.size());
                    return actionsToRun;
                }
            });

            actions.forEach(allocationContext::startNew);
        } catch (Exception e) {
            LOG.error("Failed to restore mount dynamic disk actions", e);
            throw new RuntimeException(e);
        }
    }

    private void restoreUnmountDynamicDiskActions() {
        LOG.info("Restoring unmount dynamic disk actions");
        try {
            List<UnmountDynamicDiskAction> actions = withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var deletingMounts = allocationContext.dynamicMountDao()
                        .getDeleting(allocationContext.selfWorkerId(), tx);
                    LOG.info("Found {} deleting dynamic disks", deletingMounts.size());
                    if (deletingMounts.isEmpty()) {
                        return List.of();
                    }
                    var mountsByVmId = deletingMounts.stream()
                        .collect(Collectors.groupingBy(DynamicMount::vmId));

                    var vms = allocationContext.vmDao().loadByIds(mountsByVmId.keySet(), tx);
                    Map<String, Vm> vmById = vms.stream().collect(Collectors.toMap(Vm::vmId, v -> v));
                    var actionsToRun = new ArrayList<UnmountDynamicDiskAction>(deletingMounts.size());
                    mountsByVmId.forEach((vmId, mounts) -> {
                        var vm = vmById.get(vmId);
                        for (var mount : mounts) {
                            var action = new UnmountDynamicDiskAction(vm, mount, allocationContext);
                            actionsToRun.add(action);
                        }
                    });

                    tx.commit();

                    return actionsToRun;
                }
            });
            LOG.info("Restore {} unmount dynamic disk actions", actions.size());
            actions.forEach(allocationContext::startNew);
        } catch (Exception e) {
            LOG.error("Failed to restore unmount dynamic disk actions", e);
            throw new RuntimeException(e);
        }
    }
}
