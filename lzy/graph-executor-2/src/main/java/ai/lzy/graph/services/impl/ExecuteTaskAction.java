package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.model.debug.InjectedFailures;
import ai.lzy.graph.services.AllocatorService;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.worker.MetadataConstants;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class ExecuteTaskAction extends OperationRunnerBase {
    private final AllocatorService allocatorService;
    private final Storage storage;
    private final TaskDao taskDao;
    private final Consumer<TaskState> taskOnComplete;
    private TaskState task;

    public ExecuteTaskAction(String id, TaskState task, String descr, Storage storage,
                             OperationDao operationsDao, OperationsExecutor executor, TaskDao taskDao,
                             AllocatorService allocatorService, Consumer<TaskState> taskOnComplete)
    {
        super(id, descr, storage, operationsDao, executor);
        this.allocatorService = allocatorService;
        this.storage = storage;
        this.taskDao = taskDao;
        this.taskOnComplete = taskOnComplete;

        this.task = task;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::allocate, this::awaitAllocation, this::executeOp, this::awaitExecution);
    }

    @Override
    protected final boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
    }

    @Override
    protected void notifyFinished() {
        taskOnComplete.accept(task);
    }

    private StepResult allocate() {
        final String session;
        try {
            session = withRetries(log(), () -> {
                try (TransactionHandle tx = TransactionHandle.create(storage)) {
                    var s = task.allocatorSession();

                    if (s == null) {
                        s = allocatorService.createSession(task.userId(), task.workflowName(), task.operationId());
                        task = task.toBuilder()
                            .allocatorSession(s)
                            .build();
                        taskDao.updateTask(task, tx);
                    }

                    tx.commit();
                    return s;
                }
            });
        } catch (Exception e) {
            log().error("Error while generating session in op {}", task.operationId(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        final LongRunning.Operation allocationOp;
        try {
            allocationOp = withRetries(log(), () ->
                allocatorService.allocate(task.userId(), task.workflowName(), session,
                    LMO.Requirements.newBuilder()
                        .setZone(task.taskSlotDescription().zone())
                        .setPoolLabel(task.taskSlotDescription().poolLabel())
                        .build())
            );
        } catch (Exception e) {
            log().error("Error while allocating for op {}", task.operationId(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        final String vmId;
        try {
            vmId = allocationOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        } catch (InvalidProtocolBufferException e) {
            log().error("Error while getting vmId from meta {} for task {}",
                allocationOp.getMetadata(),
                task.id(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        var updatedState = task.executingState().toBuilder()
            .allocOperationId(allocationOp.getId())
            .vmId(vmId)
            .build();
        return saveState(updatedState, TaskState.Status.ALLOCATING);
    }

    private StepResult awaitAllocation() {
        var vmDesc = allocatorService.getResponse(task.executingState().allocOperationId());
        if (vmDesc == null) {
            return StepResult.RESTART;
        }

        log().info("Vm allocated. Description is {}", JsonUtils.printRequest(vmDesc));

        var address = vmDesc.getEndpointsList()
            .stream()
            .filter(e -> e.getType().equals(VmAllocatorApi.AllocateResponse.VmEndpoint.VmEndpointType.INTERNAL_IP))
            .findFirst()
            .orElse(null);

        if (address == null) {
            log().error("Not found internal address of allocated vm {} for op {}",
                vmDesc.getVmId(), task.operationId());
            return tryFail(Status.INTERNAL.withDescription("Cannot allocate vm"));
        }

        var apiPort = vmDesc.getMetadataMap().get(MetadataConstants.API_PORT);

        if (apiPort == null) {
            log().error("Not found public api port im metadata for vm {} for op {}",
                vmDesc.getVmId(), task.operationId());
            return tryFail(Status.INTERNAL.withDescription("Cannot allocate vm"));
        }

        var updatedState = task.executingState().toBuilder()
            .fromCache(vmDesc.getFromCache())
            .workerHost(address.getValue())
            .workerPort(Integer.parseInt(apiPort))
            .build();
        return saveState(updatedState, TaskState.Status.ALLOCATING);
    }

    private StepResult executeOp() {
        var workerPrivateKey = "";

        if (!Boolean.TRUE.equals(task.executingState().fromCache())) {
            RsaUtils.RsaKeys iamKeys;
            try {
                iamKeys = RsaUtils.generateRsaKeys();
            } catch (Exception e) {
                log().error("Cannot generate RSA keys: {}", e.getMessage());
                return StepResult.RESTART;
            }

            workerPrivateKey = iamKeys.privateKey();
            try {
                allocatorService.addCredentials(task.executingState().vmId(), iamKeys.publicKey(),
                    task.userId() + "/" + task.workflowName());
            } catch (Exception e) {
                log().error("Cannot create worker: {}", e.getMessage());
                return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
            }
        }

        var host = task.executingState().workerHost();
        var port = task.executingState().workerPort();

        var client = allocatorService.createWorker(HostAndPort.fromParts(host, port));

        client.init(LWS.InitRequest.newBuilder()
            .setUserId(task.userId())
            .setWorkflowName(task.workflowName())
            .setWorkerSubjectName(task.executingState().vmId())
            .setWorkerPrivateKey(workerPrivateKey)
            .build());

        var operation = withIdempotencyKey(client, task.id())
            .execute(
                LWS.ExecuteRequest.newBuilder()
                    .setTaskId(task.id())
                    .setExecutionId(task.executionId())
                    .setWorkflowName(task.workflowName())
                    .setUserId(task.userId())
                    .setTaskDesc(task.toProto())
                    .build());

        var updatedState = task.executingState().toBuilder()
            .workerOperationId(operation.getId())
            .build();
        return saveState(updatedState, TaskState.Status.EXECUTING);
    }

    private StepResult awaitExecution() {
        var host = task.executingState().workerHost();
        var port = task.executingState().workerPort();
        var addr = HostAndPort.fromParts(host, port);

        LongRunningServiceGrpc.LongRunningServiceBlockingStub client = allocatorService.getWorker(addr);

        client = GrpcUtils.withIdempotencyKey(client, task.executingState().opId());

        final LongRunning.Operation op;
        try {
            op = client.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(task.executingState().workerOperationId())
                .build());
        } catch (StatusRuntimeException e) {
            log().error("Error while getting execution status for task {}: ", task.id(), e);
            return StepResult.RESTART;
        }

        if (!op.getDone()) {
            return StepResult.RESTART;
        }

        if (op.hasError()) {
            log().error("Task {} execution failed: [{}] {}",
                task.id(), op.getError().getCode(), op.getError().getMessage());
            return tryFail(Status.INTERNAL.withDescription(op.getError().getMessage()));
        }

        final LWS.ExecuteResponse resp;
        try {
            resp = op.getResponse().unpack(LWS.ExecuteResponse.class);
        } catch (InvalidProtocolBufferException e) {
            log().error("Error while unpacking response {} for task {}: ",
                JsonUtils.printRequest(op.getResponse()), task.id(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        final GraphExecutorApi2.TaskExecutionStatus.Builder builder = GraphExecutorApi2.TaskExecutionStatus.newBuilder()
            .setTaskDescriptionId(task.executingState().opId())
            .setOperationName(task.taskSlotDescription().name())
            .setTaskId(task.id())
            .setWorkflowId(task.executionId());

        final GraphExecutorApi2.TaskExecutionStatus status;

        if (resp.getRc() == 0) {
            status = builder
                .setSuccess(GraphExecutorApi2.TaskExecutionStatus.Success.newBuilder()
                    .setRc(0)
                    .setDescription(resp.getDescription())
                    .build()
                )
                .build();
            task = task.toBuilder()
                .status(TaskState.Status.COMPLETED)
                .build();
        } else {
            status = builder
                .setError(GraphExecutorApi2.TaskExecutionStatus.Error.newBuilder()
                    .setRc(resp.getRc())
                    .setDescription(resp.getDescription())
                    .build()
                )
                .build();
            task = task.toBuilder()
                .status(TaskState.Status.FAILED)
                .errorDescription(resp.getDescription())
                .build();
        }

        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    completeOperation(null, Any.pack(status), tx);
                    taskDao.updateTask(task, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            log().error("Sql exception while updating response for task {}", task.id(), e);
            return StepResult.RESTART;
        }

        allocatorService.free(task.executingState().vmId());
        return StepResult.FINISH;
    }

    private StepResult saveState(TaskState.ExecutingState updatedState, TaskState.Status status) {
        try {
            TaskState newTask = task.toBuilder()
                .status(status)
                .executingState(updatedState)
                .build();
            withRetries(log(), () -> taskDao.updateTask(newTask, null));
            task = newTask;
            return StepResult.CONTINUE;
        } catch (Exception e) {
            log().debug("{} Cannot save state, reschedule...", logPrefix());
            return StepResult.RESTART;
        }
    }

    private StepResult tryFail(Status status) {
        try {
            fail(status);
            return StepResult.FINISH;
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
            return StepResult.FINISH;
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
            return StepResult.FINISH;
        } catch (Exception e) {
            log().error("{} Cannot fail operation: {}", logPrefix(), e.getMessage(), e);
            return StepResult.RESTART;
        }
    }

    private void fail(Status status) throws Exception {
        log().error("{} Fail task operation: {}", logPrefix(), status.getDescription());
        task = task.toBuilder()
            .status(TaskState.Status.FAILED)
            .errorDescription(status.getDescription())
            .build();
        withRetries(log(), () -> {
            try (var tx = TransactionHandle.create(storage)) {
                failOperation(status, null);
                taskDao.updateTask(task, tx);
                tx.commit();
            }
        });
    }
}
