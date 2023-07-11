package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.fabric8.kubernetes.client.KubernetesClientException;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class DeleteVmAction extends OperationRunnerBase {
    private final String vmId;
    private final AllocationContext allocationContext;
    private Vm vm;
    private boolean tunnelDeleted = false;
    private boolean tunnelAgentDeleted = false;
    private boolean mountPodDeleted = false;
    private boolean deallocated = false;

    public DeleteVmAction(Vm vm, String deleteOpId, AllocationContext allocationContext) {
        super(deleteOpId, "VM " + vm.vmId(), allocationContext.storage(), allocationContext.operationsDao(),
            allocationContext.executor());

        this.vmId = vm.vmId();
        this.allocationContext = allocationContext;
        this.vm = vm;

        log().info("{} Delete VM...", logPrefix());
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void onExpired(TransactionHandle tx) {
        throw new RuntimeException("Unexpected, vm: '%s', op: '%s'".formatted(vmId, id()));
    }

    @Override
    protected void notifyExpired() {
        allocationContext.metrics().deleteVmErrors.inc();
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::start, this::deleteTunnel, this::deallocateTunnel,
            this::deallocateVm, this::deallocateMountPod, this::deleteAllMounts, this::cleanDb);
    }

    private StepResult start() {
        if (vm != null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            vm = withRetries(log(), () -> allocationContext.vmDao().get(vmId, null));
            if (vm == null) {
                log().error("{}: VM doesn't exist", logPrefix());
                return StepResult.FINISH;
            }

        } catch (Exception e) {
            log().error("{} Cannot load VM: {}, Reschedule...", logPrefix(), e.getMessage());
            return StepResult.RESTART;
        }

        return switch (vm.status()) {
            case DELETING -> StepResult.CONTINUE;
            case RUNNING, IDLE -> {
                if (vm.runState().activityDeadline().isBefore(Instant.now())) {
                    // do nothing for expired Vm
                    yield StepResult.CONTINUE;
                }

                // TODO: some logic of communicating with VM's allocator agent should be here
                // for Lzy: ensure all slots are flushed
                yield StepResult.CONTINUE;
            }
            case ALLOCATING -> throw new IllegalStateException("Unexpected value: " + vm.status());
        };
    }

    private StepResult deleteTunnel() {
        var name = vm.instanceProperties().tunnelPodName();
        if (name == null) {
            return StepResult.ALREADY_DONE;
        }

        if (tunnelDeleted) {
            return StepResult.ALREADY_DONE;
        }

        var allocatorMeta = vm.allocateState().allocatorMeta();
        if (allocatorMeta == null) {
            log().warn("{} Allocator meta for vm {} is null", logPrefix(), vm.vmId());
            return StepResult.ALREADY_DONE;
        }

        var clusterId = allocatorMeta.get(KuberVmAllocator.CLUSTER_ID_KEY);
        if (clusterId == null) {
            log().warn("{} Cluster id isn't found for vm {}", logPrefix(), vm.vmId());
            return StepResult.ALREADY_DONE;
        }

        try {
            VmAllocator.Result result = allocationContext.tunnelAllocator().deleteTunnel(clusterId, name);
            return switch (result.code()) {
                case SUCCESS -> {
                    tunnelDeleted = true;
                    yield StepResult.CONTINUE;
                }
                case RETRY_LATER, FAILED -> {
                    log().error("{} Cannot delete tunnel on tunnel pod {}: {}", logPrefix(), name, result.message());
                    yield StepResult.RESTART;
                }
            };
        } catch (Exception e) {
            log().error("{} Cannot delete tunnel on tunnel pod {}: {}", logPrefix(), name, e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private StepResult deallocateTunnel() {
        var name = vm.instanceProperties().tunnelPodName();
        if (name == null) {
            return StepResult.ALREADY_DONE;
        }

        if (tunnelAgentDeleted) {
            return StepResult.ALREADY_DONE;
        }

        var allocatorMeta = vm.allocateState().allocatorMeta();
        if (allocatorMeta == null) {
            log().warn("{} Allocator meta for vm {} is null", logPrefix(), vm.vmId());
            return StepResult.ALREADY_DONE;
        }
        var clusterId = allocatorMeta.get(KuberVmAllocator.CLUSTER_ID_KEY);
        if (clusterId == null) {
            log().warn("{} Cluster id isn't found for vm {}", logPrefix(), vm.vmId());
            return StepResult.ALREADY_DONE;
        }

        try {
            var result = allocationContext.tunnelAllocator().deallocateTunnelAgent(clusterId, name);
            return switch (result.code()) {
                case SUCCESS -> {
                    tunnelAgentDeleted = true;
                    yield StepResult.CONTINUE;
                }
                case RETRY_LATER, FAILED -> {
                    log().error("{} Cannot delete tunnel pod {}. Reason: {}", logPrefix(), name, result.message());
                    yield StepResult.RESTART;
                }
            };
        } catch (Exception e) {
            log().error("{} Cannot delete tunnel pod {}: {}", logPrefix(), name, e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private StepResult deallocateVm() {
        if (deallocated) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Trying to deallocate VM...", logPrefix());
        try {
            var ret = allocationContext.allocator().deallocate(vm);
            return switch (ret.code()) {
                case SUCCESS -> {
                    deallocated = true;
                    log().info("{} VM deallocated", logPrefix());
                    yield StepResult.CONTINUE;
                }
                case RETRY_LATER -> {
                    log().error("{} Cannot deallocate VM: {}. Retry later...", logPrefix(), ret.message());
                    yield StepResult.RESTART;
                }
                case FAILED -> {
                    log().error("{} Deallocation failed: {}", logPrefix(), ret.message());
                    allocationContext.metrics().deleteVmErrors.inc();
                    yield StepResult.FINISH;
                }
            };
        } catch (Exception e) {
            log().error("{} Error while deallocating VM: {}", logPrefix(), e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private StepResult deallocateMountPod() {
        if (!allocationContext.mountConfig().isEnabled()) {
            return StepResult.ALREADY_DONE;
        }

        if (mountPodDeleted) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Trying to deallocate mount pods...", logPrefix());
        try {
            allocationContext.mountHolderManager().deallocateAllMountPods(vm.spec());
            mountPodDeleted = true;
        } catch (KubernetesClientException e) {
            log().error("{} Error while deallocating mount pods: {}", logPrefix(), e.getMessage(), e);
            if (KuberUtils.isNotRetryable(e)) {
                return StepResult.CONTINUE;
            }
            return StepResult.RESTART;
        }
        return StepResult.CONTINUE;
    }

    private StepResult deleteAllMounts() {
        if (!allocationContext.mountConfig().isEnabled()) {
            return StepResult.ALREADY_DONE;
        }
        try {
            var unmountActions = withRetries(log(), () -> {
                var actions = new ArrayList<Runnable>();
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    var mounts = allocationContext.dynamicMountDao().getByVm(vm.vmId(), tx);
                    for (var mount : mounts) {
                        if (mount.state() == DynamicMount.State.DELETING) {
                            continue;
                        }
                        var unmountActionWithOp = allocationContext.createUnmountAction(vm, mount, tx);
                        actions.add(unmountActionWithOp.getLeft());
                    }
                    tx.commit();
                    return actions;
                }
            });
            for (var action : unmountActions) {
                allocationContext.startNew(action);
            }
        } catch (Exception e) {
            log().error("{} Cannot delete all vm {} mounts: {}", logPrefix(), vmId, e.getMessage(), e);
            //don't retry this step because we can remove all garbage mounts later
        }
        return StepResult.CONTINUE;
    }

    private StepResult cleanDb() {
        try (var tx = TransactionHandle.create(allocationContext.storage())) {
            completeOperation(null, Any.pack(Empty.getDefaultInstance()), tx);
            allocationContext.vmDao().cleanupVm(vm.vmId(), tx);

            // TODO: move to GC
            // allocationContext.operationsDao().deleteCompletedOperation(vm.allocateState().operationId(), tx);
            // allocationContext.operationsDao().deleteCompletedOperation(vm.deleteState().operationId(), tx);

            tx.commit();
        } catch (SQLException e) {
            log().error("{} Cannot cleanup DB after deleting VM: {}", logPrefix(), e.getMessage());
            return StepResult.RESTART;
        }
        log().info("{} VM deleted", logPrefix());
        return StepResult.FINISH;
    }
}
