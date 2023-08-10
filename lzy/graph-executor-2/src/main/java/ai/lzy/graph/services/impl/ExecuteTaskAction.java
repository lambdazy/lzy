package ai.lzy.graph.services.impl;

import ai.lzy.graph.LGE;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.model.debug.InjectedFailures;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.logs.LogContextKey;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import ai.lzy.worker.MetadataConstants;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.Objects.requireNonNull;

public class ExecuteTaskAction extends OperationRunnerBase {
    private final WorkerService workerService;
    private final Storage storage;
    private final TaskDao taskDao;
    private final Consumer<TaskState> taskOnComplete;
    private TaskState task;

    public ExecuteTaskAction(String id, TaskState task, String descr, Storage storage,
                             OperationDao operationsDao, OperationsExecutor executor, TaskDao taskDao,
                             WorkerService workerService, Consumer<TaskState> taskOnComplete)
    {
        super(id, descr, storage, operationsDao, executor);
        this.workerService = workerService;
        this.storage = storage;
        this.taskDao = taskDao;
        this.taskOnComplete = taskOnComplete;
        this.task = task;

        var executingState = requireNonNull(task.executingState());
        if (executingState.workerOperationId() != null) {
            workerService.restoreWorker(
                requireNonNull(executingState.vmId()),
                requireNonNull(executingState.workerHost()),
                executingState.workerPort());
        }
    }

    @Override
    protected Map<String, String> prepareLogContext() {
        var map = super.prepareLogContext();
        map.put(LogContextKey.EXECUTION_TASK_ID, task.id());
        return map;
    }

    @Override
    protected Map<Metadata.Key<String>, String> prepareGrpcHeaders() {
        var map = super.prepareGrpcHeaders();
        map.put(GrpcHeaders.X_EXECUTION_TASK_ID, task.id());
        return map;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::allocateVm, this::awaitVmAllocation, this::executeOp, this::awaitExecution, this::cleanup);
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
        var vmId = executingState().vmId();
        if (vmId != null) {
            workerService.freeVm(vmId);
        }

        taskOnComplete.accept(task);
    }

    private TaskState.ExecutingState executingState() {
        return requireNonNull(task.executingState());
    }

    private StepResult allocateVm() {
        if (executingState().allocOperationId() != null) {
            return StepResult.ALREADY_DONE;
        }

        final LongRunning.Operation allocationOp;
        try {
            allocationOp = workerService.allocateVm(
                task.allocatorSessionId(),
                LMO.Requirements.newBuilder()
                    .setZone(task.taskSlotDescription().zone())
                    .setPoolLabel(task.taskSlotDescription().poolLabel())
                    .build(),
                "%s/%s/%s".formatted(task.userId(), task.executionId(), task.id()));
        } catch (StatusRuntimeException e) {
            log().error("Error while allocating VM for op {}: {}", task.operationId(), e.getStatus());
            return StepResult.RESTART;
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

        var newTask = task.toStartAllocation(allocationOp.getId(), vmId);
        return saveState(newTask);
    }

    private StepResult awaitVmAllocation() {
        if (executingState().fromCache() != null) {
            return StepResult.ALREADY_DONE;
        }

        var allocOpId = requireNonNull(executingState().allocOperationId());

        var allocOp = workerService.getAllocOp(allocOpId);
        if (allocOp == null) {
            log().error("Allocation op {} is lost", allocOpId);
            return tryFail(Status.INTERNAL.withDescription("Allocation operation is lost"));
        }

        if (!allocOp.getDone()) {
            return StepResult.RESTART;
        }

        if (allocOp.hasError()) {
            log().error("Allocation failed, vmId: {}, error: {}", executingState().vmId(), allocOp.getError());
            return tryFail(Status.fromCodeValue(allocOp.getError().getCode())
                .withDescription(allocOp.getError().getMessage()));
        }

        VmAllocatorApi.AllocateResponse allocVm;
        try {
            allocVm = allocOp.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        } catch (InvalidProtocolBufferException e) {
            log().error("Cannot unpack allocation result for vm {}: {}", executingState().vmId(), e.getMessage());
            return tryFail(Status.INTERNAL.withDescription("Proto format error: " + e.getMessage()));
        }

        log().info("Vm {} allocated: {}", executingState().vmId(), ProtoPrinter.safePrinter().printToString(allocVm));

        var address = allocVm.getEndpointsList().stream()
            .filter(e -> e.getType().equals(VmAllocatorApi.AllocateResponse.VmEndpoint.VmEndpointType.INTERNAL_IP))
            .map(VmAllocatorApi.AllocateResponse.VmEndpoint::getValue)
            .findFirst()
            .orElse(null);

        if (address == null) {
            log().error("No internal address for allocated vm {} for op {}", allocVm.getVmId(), task.operationId());
            return tryFail(Status.INTERNAL.withDescription("Cannot allocate vm"));
        }

        var apiPort = allocVm.getMetadataMap().get(MetadataConstants.API_PORT);

        if (apiPort == null) {
            log().error("Not api port im metadata for vm {} for op {}", allocVm.getVmId(), task.operationId());
            return tryFail(Status.INTERNAL.withDescription("Cannot allocate vm"));
        }

        var newTask = task.toExecutingState(address, Integer.parseInt(apiPort), allocVm.getFromCache());
        return saveState(newTask);
    }

    private StepResult executeOp() {
        if (executingState().workerOperationId() != null) {
            return StepResult.ALREADY_DONE;
        }

        var vmId = requireNonNull(executingState().vmId());

        var workerPrivateKey = "";
        if (!executingState().fromCache()) {
            RsaUtils.RsaKeys iamKeys;
            try {
                iamKeys = RsaUtils.generateRsaKeys();
            } catch (Exception e) {
                log().error("Cannot generate RSA keys: {}", e.getMessage());
                return StepResult.RESTART;
            }

            workerPrivateKey = iamKeys.privateKey();
            try {
                workerService.createWorkerSubject(vmId, iamKeys.publicKey(), task.userId() + "/" + task.workflowName());
            } catch (Exception e) {
                log().error("Cannot create worker: {}", e.getMessage());
                return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
            }
        }

        var host = executingState().workerHost();
        var port = executingState().workerPort();

        try {
            workerService.init(vmId, task.userId(), task.workflowName(), host, port, workerPrivateKey);
        } catch (StatusRuntimeException e) {
            if (GrpcUtils.retryableStatusCode(e.getStatus())) {
                log().error("Cannot init worker, retry later: {}", e.getStatus());
                return StepResult.RESTART;
            }
            return tryFail(Status.INTERNAL);
        }

        var execOp = workerService.execute(
            vmId,
            LWS.ExecuteRequest.newBuilder()
                .setTaskId(task.id())
                .setExecutionId(task.executionId())
                .setWorkflowName(task.workflowName())
                .setUserId(task.userId())
                .setTaskDesc(task.toProto())
                .build(),
            "idk-" + task.id());

        var newTask = task.toExecutingState(execOp.getId());
        return saveState(newTask);
    }

    private StepResult awaitExecution() {
        var vmId = requireNonNull(executingState().vmId());

        var execOp = workerService.getWorkerOp(vmId, executingState().opId());
        if (execOp == null) {
            log().error("Cannot find exec op for vmId {}", vmId);
            return tryFail(Status.INTERNAL);
        }

        if (!execOp.getDone()) {
            log().debug("Waiting task {}", task);
            return StepResult.RESTART.after(Duration.ofSeconds(5));
        }

        if (execOp.hasError()) {
            log().error("Task {} execution failed: [{}] {}",
                task.id(), execOp.getError().getCode(), execOp.getError().getMessage());
            return tryFail(Status.INTERNAL.withDescription(execOp.getError().getMessage()));
        }

        if (task.status() == TaskState.Status.EXECUTING) {
            final LWS.ExecuteResponse resp;
            try {
                resp = execOp.getResponse().unpack(LWS.ExecuteResponse.class);
            } catch (InvalidProtocolBufferException e) {
                log().error("Error while unpacking response {} for task {}: ",
                    JsonUtils.printRequest(execOp.getResponse()), task.id(), e);
                return tryFail(Status.INTERNAL.withDescription(e.getMessage()));
            }

            var builder = LGE.TaskExecutionStatus.newBuilder()
                .setTaskId(task.id())
                .setTaskDescriptionId(executingState().opId())
                .setWorkflowName(task.workflowName())
                .setExecutionId(task.executionId())
                .setOperationName(task.taskSlotDescription().name());

            final LGE.TaskExecutionStatus status;

            if (resp.getRc() == 0) {
                status = builder
                    .setSuccess(LGE.TaskExecutionStatus.Success.newBuilder()
                        .setRc(0)
                        .setDescription(resp.getDescription())
                        .build())
                    .build();
                task.complete();
            } else {
                status = builder
                    .setError(LGE.TaskExecutionStatus.Error.newBuilder()
                        .setRc(resp.getRc())
                        .setDescription(resp.getDescription())
                        .build())
                    .build();
                task.fail(resp.getDescription());
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
        }

        return StepResult.CONTINUE;
    }

    private StepResult cleanup() {
        workerService.freeVm(executingState().vmId());
        return StepResult.FINISH;
    }

    private StepResult saveState(TaskState newState) {
        try {
            withRetries(log(), () -> taskDao.updateTask(newState, null));
            task = newState;
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
        task.fail(status.getDescription());
        withRetries(log(), () -> {
            try (var tx = TransactionHandle.create(storage)) {
                failOperation(status, tx);
                taskDao.updateTask(task, tx);
                tx.commit();
            }
        });
    }
}
