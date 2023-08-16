package ai.lzy.graph.services.impl;

import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.WorkerService;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.function.Supplier;

import static com.google.protobuf.TextFormat.printer;
import static java.util.Objects.requireNonNull;

public class StopTaskAction extends OperationRunnerBase {
    private TaskState task;
    private final WorkerService workerService;

    public StopTaskAction(String id, TaskState task, String descr, Storage storage, OperationDao operationsDao,
                          OperationsExecutor executor, WorkerService workerService)
    {
        super(id, descr, storage, operationsDao, executor);
        this.task = task;
        this.workerService = workerService;

        assert task.status() == TaskState.Status.FAILED;

        log().info("{} Stop running task {}", logPrefix(), task);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::cancelWorkerOp, this::freeVm);
    }

    private StepResult cancelWorkerOp() {
        var execState = executingState();
        if (execState.workerOperationId() == null) {
            return StepResult.ALREADY_DONE;
        }

        var op = workerService.cancelWorkerOp(requireNonNull(execState.vmId()), execState.workerOperationId());
        if (op != null) {
            log().debug("{} Worker operation {}: {}",
                logPrefix(), op.getId(), op.hasError() ? printer().shortDebugString(op.getError()) : "completed");
        }

        task = task.withExecutingState(execState.clearWorker());

        return StepResult.CONTINUE;
    }

    private StepResult freeVm() {
        var execState = executingState();
        if (execState.vmId() == null) {
            return cancelAllocVm();
        }

        doFreeVm(execState.vmId());
        return StepResult.CONTINUE;
    }

    private StepResult cancelAllocVm() {
        if (executingState().allocOperationId() == null) {
            return StepResult.ALREADY_DONE;
        }

        var op = workerService.cancelAllocOp(
            requireNonNull(executingState().allocOperationId()),
            task.errorDescription() != null ? task.errorDescription() : "cancelled");

        if (op != null) {
            assert op.getDone();

            if (op.hasError()) {
                return StepResult.FINISH;
            }

            String vmId = null;
            try {
                vmId = op.getResponse().unpack(VmAllocatorApi.AllocateResponse.class).getVmId();
            } catch (InvalidProtocolBufferException e) {
                log().error("{} Cannot unpack proto {}: {}", logPrefix(), op.getResponse(), e.getMessage());
            }

            if (vmId != null) {
                doFreeVm(vmId);
            }
        }

        return StepResult.FINISH;
    }

    private void doFreeVm(String vmId) {
        try {
            workerService.freeVm(vmId);
        } catch (StatusRuntimeException e) {
            log().error("{} Cannot free VM {}: {}", logPrefix(), vmId, e.getStatus());
        }
        task = task.withExecutingState(executingState().clearVm());
    }

    private TaskState.ExecutingState executingState() {
        return requireNonNull(task.executingState());
    }

}
