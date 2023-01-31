package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMetrics;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc;

import java.sql.SQLException;

import static ai.lzy.model.db.DbHelper.withRetries;

abstract class YcDiskActionBase<S> extends OperationRunnerBase {

    protected final DiskManager.OuterOperation op;
    protected S state;
    protected final YcDiskManager diskManager;

    protected YcDiskActionBase(DiskManager.OuterOperation op, String cmd, S state, YcDiskManager dm) {
        super(op.opId(), cmd + " " + op.descr(), dm.storage(), dm.operationsDao(), dm.executor());
        this.op = op;
        this.state = state;
        this.diskManager = dm;
    }

    @Override
    protected final boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        diskOpDao().deleteDiskOp(opId(), tx);
    }

    protected final StepResult saveState(Runnable onSuccess, Runnable onFail) {
        try {
            withRetries(log(), () -> diskOpDao().updateDiskOp(opId(), toJson(state), null));
            onSuccess.run();
            return StepResult.CONTINUE;
        } catch (Exception e) {
            log().debug("[{}] Cannot save state for op {}, reschedule...", descr(), opId());
            onFail.run();
            return StepResult.RESTART;
        }
    }

    protected final String opId() {
        return op.opId();
    }

    protected final String toJson(Object obj) {
        try {
            return diskManager.objectMapper().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected final AllocatorDataSource storage() {
        return diskManager.storage();
    }

    protected final DiskDao diskDao() {
        return diskManager.diskDao();
    }

    protected final DiskOpDao diskOpDao() {
        return diskManager.diskOpDao();
    }

    protected final DiskMetrics metrics() {
        return diskManager.metrics();
    }

    protected final DiskServiceGrpc.DiskServiceBlockingStub ycDiskService() {
        return diskManager.ycDiskService();
    }

    protected final SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService() {
        return diskManager.ycSnapshotService();
    }

    protected final OperationServiceGrpc.OperationServiceBlockingStub ycOperationService() {
        return diskManager.ycOperationService();
    }
}
