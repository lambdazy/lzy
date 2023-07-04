package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.model.debug.InjectedFailures;
import ai.lzy.graph.services.AllocatorService;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmAllocatorApi;
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
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class ExecuteTaskAction extends OperationRunnerBase {
    private final AllocatorService allocatorService;
    private final Storage storage;
    private final TaskDao taskDao;
    private final OperationDao operationDao;

    private TaskOperation taskOp;
    private TaskState task;

    public ExecuteTaskAction(String id, TaskOperation taskOp, TaskState task, String descr, Storage storage,
                             OperationDao operationsDao, OperationsExecutor executor, TaskDao taskDao,
                             AllocatorService allocatorService)
    {
        super(id, descr, storage, operationsDao, executor);
        this.allocatorService = allocatorService;
        this.storage = storage;

        this.taskOp = taskOp;
        this.taskDao = taskDao;
        this.operationDao = operationsDao;
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
    }

    private StepResult allocate() {
        final String session;
        try {
            session = DbHelper.withRetries(log(), () -> {
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

        var allocationOp = allocatorService.allocate(task.userId(), task.workflowName(), session,
            task.taskSlotDescription().requirements());

        final String vmId;
        try {
            vmId = allocationOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        } catch (InvalidProtocolBufferException e) {
            log().error("Error while getting vmId from meta {} for task {}",
                allocationOp.getMetadata(),
                task.id(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        var updatedState = taskOp.state().toBuilder()
            .allocOperationId(allocationOp.getId())
            .vmId(vmId)
            .build();
        return saveState(updatedState, TaskOperation.Status.ALLOCATING);
    }

    private StepResult awaitAllocation() {
        var vmDesc = allocatorService.getResponse(taskOp.state().allocOperationId());
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

        var updatedState = taskOp.state().toBuilder()
            .fromCache(vmDesc.getFromCache())
            .workerHost(address.getValue())
            .workerPort(Integer.parseInt(apiPort))
            .build();
        return saveState(updatedState, TaskOperation.Status.ALLOCATING);
    }

    private StepResult executeOp() {
        var workerPrivateKey = "";

        if (!Boolean.TRUE.equals(taskOp.state().fromCache())) {
            RsaUtils.RsaKeys iamKeys;
            try {
                iamKeys = RsaUtils.generateRsaKeys();
            } catch (Exception e) {
                log().error("Cannot generate RSA keys: {}", e.getMessage());
                return StepResult.RESTART;
            }

            workerPrivateKey = iamKeys.privateKey();
            try {
                allocatorService.addCredentials(taskOp.state().vmId(), iamKeys.publicKey(),
                    task.userId() + "/" + task.workflowName());
            } catch (Exception e) {
                log().error("Cannot create worker: {}", e.getMessage());
                return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
            }
        }

        var host = taskOp.state().workerHost();
        var port = taskOp.state().workerPort();

        var client = allocatorService.createWorker(HostAndPort.fromParts(host, port));

        client.init(LWS.InitRequest.newBuilder()
            .setUserId(task.userId())
            .setWorkflowName(task.workflowName())
            .setWorkerSubjectName(taskOp.state().vmId())
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

        var updatedState = taskOp.state().toBuilder()
            .workerOperationId(operation.getId())
            .build();
        return saveState(updatedState, TaskOperation.Status.EXECUTING);
    }

    private StepResult awaitExecution() {
        var host = taskOp.state().workerHost();
        var port = taskOp.state().workerPort();
        var addr = HostAndPort.fromParts(host, port);

        LongRunningServiceGrpc.LongRunningServiceBlockingStub client = allocatorService.getWorker(addr);

        client = GrpcUtils.withIdempotencyKey(client, taskOp.id());

        final LongRunning.Operation op;
        try {
            op = client.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(taskOp.state().workerOperationId())
                .build());
        } catch (StatusRuntimeException e) {
            log().error("Error while getting execution status for task op {}: ", taskOp.id(), e);
            return StepResult.RESTART;
        }

        if (!op.getDone()) {
            return StepResult.RESTART;
        }

        if (op.hasError()) {
            log().error("Task op {} execution failed: [{}] {}",
                taskOp.id(), op.getError().getCode(), op.getError().getMessage());
            return tryFail(Status.INTERNAL.withDescription(op.getError().getMessage()));
        }

        final LWS.ExecuteResponse resp;
        try {
            resp = op.getResponse().unpack(LWS.ExecuteResponse.class);
        } catch (InvalidProtocolBufferException e) {
            log().error("Error while unpacking response {} for task op {}: ",
                JsonUtils.printRequest(op.getResponse()), taskOp.id(), e);
            return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
        }

        final GraphExecutorApi2.TaskExecutionStatus.Builder builder = GraphExecutorApi2.TaskExecutionStatus.newBuilder()
            .setTaskDescriptionId(taskOp.id())
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
        } else {
            status = builder
                .setError(GraphExecutorApi2.TaskExecutionStatus.Error.newBuilder()
                    .setRc(resp.getRc())
                    .setDescription(resp.getDescription())
                    .build()
                )
                .build();
        }

        try {
            DbHelper.withRetries(log(), () -> {
                operationDao.complete(taskOp.id(), Any.pack(status), null);
            });
        } catch (Exception e) {
            log().error("Sql exception while updating response for task {}", taskOp.id(), e);
            return StepResult.RESTART;
        }

        allocatorService.free(taskOp.state().vmId());
        return StepResult.FINISH;
    }

    private StepResult saveState(TaskOperation.State updatedState, TaskOperation.Status status) {
        try {
            TaskOperation newOp = taskOp.toBuilder()
                .status(status)
                .state(updatedState)
                .build();
            withRetries(log(), () -> taskDao.updateTaskOperation(newOp, null));
            taskOp = newOp;
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
        withRetries(log(), () -> failOperation(status, null));
    }
}
