package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.model.Session;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.allocator.util.KuberUtils.isNotRetryable;
import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;

public class DeleteSessionAction extends OperationRunnerBase {
    private final String sessionId;
    private final String reqid;
    private final AllocationContext allocationContext;
    private final SessionDao sessionDao;

    public DeleteSessionAction(Session session, String opId, AllocationContext allocationContext,
                               SessionDao sessionDao)
    {
        super(opId, "Sid " + session.sessionId(), allocationContext.storage(),
            allocationContext.operationsDao(), allocationContext.executor());

        this.sessionId = session.sessionId();
        this.reqid = session.deleteReqid();
        this.allocationContext = allocationContext;
        this.sessionDao = sessionDao;

        log().info("Delete session " + session.sessionId());
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
    }

    @Override
    protected void notifyFinished() {
    }

    @Override
    protected void onExpired(TransactionHandle tx) {
        throw new RuntimeException("Unexpected, sessionId: '%s', op: '%s'".formatted(sessionId, id()));
    }

    @Override
    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
        sessionDao.removeSession(sessionId, tx);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::removeVMs, this::removeNetPolicy, this::removeSession);
    }

    private StepResult removeVMs() {
        List<Vm> vms;
        try {
            vms = withRetries(log(), () -> allocationContext.vmDao().getSessionVms(sessionId, null));
        } catch (Exception e) {
            log().error("{} Cannot load session's vms: {}", logPrefix(), e.getMessage());
            return StepResult.RESTART;
        }

        if (vms.isEmpty()) {
            return StepResult.CONTINUE;
        }

        for (var vm : vms) {
            switch (vm.status()) {
                case ALLOCATING -> {
                    try {
                        withRetries(log(), () -> operationsDao()
                            .fail(vm.allocOpId(), toProto(Status.CANCELLED.withDescription("Session removed")), null));
                    } catch (OperationCompletedException e) {
                        log().warn("{} Failed to abort VM {} allocation {}: already completed",
                            logPrefix(), vm.vmId(), vm.allocOpId());
                    } catch (NotFoundException e) {
                        log().warn("{} Failed to abort VM {} allocation {}: not found",
                            logPrefix(), vm.vmId(), vm.allocOpId());
                    } catch (Exception e) {
                        log().error("{} Failed to abort VM {} allocation {}: {}",
                            logPrefix(), vm.vmId(), vm.allocOpId(), e.getMessage());
                        return StepResult.RESTART;
                    }
                }

                case RUNNING, IDLE -> {
                    try {
                        allocationContext.startDeleteVmAction(
                            vm, "Delete VM %s on session %s remove".formatted(vm, sessionId), reqid, log());
                    } catch (Exception e) {
                        log().error("{} Cannot cleanup expired VM {}: {}", logPrefix(), vm.vmId(), e.getMessage());
                        return StepResult.RESTART;
                    }

                }

                case DELETING -> log().info("{} VM {} already deleting", logPrefix(), vm.vmId());
            }
        }

        return StepResult.RESTART;
    }

    private StepResult removeNetPolicy() {
        try {
            allocationContext.networkPolicyManager().deleteNetworkPolicy(sessionId);
        } catch (Exception e) {
            if (e instanceof KubernetesClientException && !isNotRetryable((KubernetesClientException) e)) {
                return StepResult.RESTART;
            }
            log().error("Cannot remove net-policy {} : {}", sessionId, e.getMessage(), e);
            return StepResult.FINISH;
        }
        return StepResult.CONTINUE;
    }

    private StepResult removeSession() {
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(allocationContext.storage())) {
                    completeOperation(null, Any.pack(Empty.getDefaultInstance()), tx);
                    sessionDao.removeSession(sessionId, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            log().error("{} Cannot complete operation: {}", logPrefix(), e.getMessage(), e);
            return StepResult.RESTART;
        }
        log().info("{} Operation completed", logPrefix());
        return StepResult.FINISH;
    }
}
