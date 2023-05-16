package ai.lzy.service.workflow.start;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class StartExecutionAction extends OperationRunnerBase {
    private final WorkflowDao wfDao;
    private final ExecutionDao execDao;

    private final String userId;
    private final String wfName;
    private final String execId;

    private String allocOpId = null;
    private final boolean isKafkaEnabled;
    private final Duration allocatorVmCacheTimeout;
    private final AllocPortalVmSpec allocPortalVmSpec;

    private final String idempotencyKey;
    private final KafkaAdminClient kafkaAdminClient;
    private final AllocatorBlockingStub allocClient;
    private final SubjectServiceGrpcClient subjClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final LongRunningServiceBlockingStub allocOpClient;

    private final ActionsManager actionsManager;
    private final LzyServiceMetrics metrics;
    private final Function<StatusRuntimeException, StepResult> failAction;

    public StartExecutionAction(String id, String descr, String userId, String wfName, String execId, Storage storage,
                                OperationDao operationsDao, WorkflowDao wfDao, ExecutionDao execDao,
                                OperationsExecutor executor, boolean isKafkaEnabled, Duration allocatorVmCacheTimeout,
                                @Nullable String idempotencyKey, SubjectServiceGrpcClient subjClient,
                                AccessBindingServiceGrpcClient abClient, KafkaAdminClient kafkaAdminClient,
                                AllocatorBlockingStub allocClient, LongRunningServiceBlockingStub allocOpClient,
                                ActionsManager actionsManager, LzyServiceMetrics metrics)
    {
        super(id, descr, storage, operationsDao, executor);
        this.wfDao = wfDao;
        this.execDao = execDao;
        this.userId = userId;
        this.wfName = wfName;
        this.execId = execId;
        this.isKafkaEnabled = isKafkaEnabled;
        this.allocatorVmCacheTimeout = allocatorVmCacheTimeout;
        this.allocPortalVmSpec = new AllocPortalVmSpec();
        this.idempotencyKey = idempotencyKey;
        this.kafkaAdminClient = kafkaAdminClient;
        this.allocClient = allocClient;
        this.subjClient = subjClient;
        this.abClient = abClient;
        this.allocOpClient = allocOpClient;
        this.actionsManager = actionsManager;
        this.metrics = metrics;
        this.failAction = sre -> fail(sre.getStatus()) ? StepResult.FINISH : StepResult.RESTART;
    }

    private void setAllocOpId(String opId) {
        this.allocOpId = opId;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return new ArrayList<>() {
            {
                if (isKafkaEnabled) {
                    add(createKafkaTopic());
                }
                add(createAllocatorSession());
                add(createPortalSubject());
                add(startAllocationPortalVm());
                add(waitAllocationPortalVm());
                add(StartExecutionAction.this::complete);
            }
        };
    }

    private Supplier<StepResult> createKafkaTopic() {
        return new CreateKafkaTopic(execDao, execId, kafkaAdminClient, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> createAllocatorSession() {
        return new CreateAllocatorSession(execDao, userId, wfName, execId, allocatorVmCacheTimeout, idempotencyKey,
            allocClient, allocPortalVmSpec::setSessionId, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> createPortalSubject() {
        return new CreatePortalSubject(execDao, userId, execId, wfName, subjClient, abClient,
            allocPortalVmSpec::setPortalId, allocPortalVmSpec::setSessionId, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> startAllocationPortalVm() {
        return new StartAllocationPortalVm(execDao, wfName, execId, idempotencyKey, allocClient, allocOpClient,
            allocPortalVmSpec, this::setAllocOpId, failAction, log(), logPrefix());
    }

    private Supplier<StepResult> waitAllocationPortalVm() {
        return new WaitAllocationPortalVm(execDao, execId, allocOpId, allocOpClient, failAction, log(), logPrefix());
    }

    private StepResult complete() {
        var response = Any.pack(LWFS.StartWorkflowResponse.newBuilder().setExecutionId(execId).build());
        try {
            withRetries(log(), () -> completeOperation(null, response, null));
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful StartWorkflow operation: {}.{}", logPrefix(), e.getMessage(),
                (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        metrics.activeExecutions.labels(userId).inc();
        return StepResult.FINISH;
    }

    private boolean fail(Status status) {
        log().error("{} Fail StartWorkflow operation: {}", logPrefix(), status.getDescription());

        boolean[] success = {false};
        var stopOp = Operation.create(userId, "Stop execution: execId='%s'".formatted(execId),
            AbortExecutionAction.timeout, null, null);
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    success[0] = Objects.equals(wfDao.getExecutionId(userId, wfName, tx), execId);
                    if (success[0]) {
                        wfDao.setActiveExecutionId(userId, wfName, null, tx);
                        operationsDao().create(stopOp, tx);
                    }
                    failOperation(status, tx);
                    tx.commit();
                }
            });
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
            return true;
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
            return true;
        } catch (Exception ex) {
            log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
            return false;
        }

        if (success[0]) {
            try {
                log().debug("{} Schedule action to abort execution that not started properly: { execId: {} }",
                    logPrefix(), execId);
                actionsManager.abortExecutionAction(stopOp.id(), stopOp.description(), null, execId,
                    Status.INTERNAL.withDescription("error on start"));
            } catch (Exception e) {
                log().warn("{} Cannot schedule action to abort execution that not startd properly: { execId: {}, " +
                    "error: {} }", logPrefix(), execId, e.getMessage(), e);
            }
        }

        return true;
    }
}

