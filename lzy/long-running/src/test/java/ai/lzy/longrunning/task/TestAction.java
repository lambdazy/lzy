package ai.lzy.longrunning.task;

import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.task.dao.OperationTaskDao;
import ai.lzy.model.db.Storage;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.grpc.Status;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

public class TestAction extends OpTaskAwareAction {

    private final Supplier<StepResult>  action;
    private final boolean completeOperation;

    public TestAction(Supplier<StepResult> action, boolean completeOperation, OperationTask operationTask,
                      OperationTaskDao operationTaskDao, Duration leaseDuration, String opId, String desc,
                      Storage storage, OperationDao operationsDao,
                      OperationsExecutor executor,
                      OperationTaskScheduler operationTaskScheduler)
    {
        super(operationTask, operationTaskDao, leaseDuration, opId, desc, storage, operationsDao, executor,
            operationTaskScheduler);
        this.action = action;
        this.completeOperation = completeOperation;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(this::doSmth);
    }

    private StepResult doSmth() {
        var stepResult = action.get();
        try {
            if (completeOperation) {
                completeOperation(null, Any.pack(Empty.getDefaultInstance()), null);
            } else {
                failOperation(Status.INTERNAL, null);
            }
        } catch (SQLException e) {
            log().error("{} Error while completing operation", logPrefix());
        }
        return stepResult;
    }
}
