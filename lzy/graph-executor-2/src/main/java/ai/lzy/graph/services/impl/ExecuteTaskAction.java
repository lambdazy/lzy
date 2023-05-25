package ai.lzy.graph.services.impl;

import java.util.List;
import java.util.function.Supplier;

import ai.lzy.graph.model.debug.InjectedFailures;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;

public class ExecuteTaskAction extends OperationRunnerBase {
    protected ExecuteTaskAction(String id, String descr, Storage storage,
                                OperationDao operationsDao,
                                OperationsExecutor executor)
    {
        super(id, descr, storage, operationsDao, executor);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return null;
    }

    @Override
    protected final boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

}
