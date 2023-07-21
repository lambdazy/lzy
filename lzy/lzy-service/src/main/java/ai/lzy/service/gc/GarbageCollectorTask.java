package ai.lzy.service.gc;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.service.operations.OperationRunnersFactory;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);

    private final String gcId;
    @Nullable
    private final GarbageCollectorInterceptor interceptor;
    private final OperationRunnersFactory operationRunnersFactory;
    private final AtomicBoolean terminate = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public GarbageCollectorTask(String id, @Nullable GarbageCollectorInterceptor interceptor,
                                OperationRunnersFactory operationRunnersFactory)
    {
        this.gcId = id;
        this.interceptor = interceptor;
        this.operationRunnersFactory = operationRunnersFactory;
    }

    @Override
    public void run() {
        running.set(true);

        try {
            var outdatedSessions = operationRunnersFactory.wfDao().listOutdatedAllocatorSessions(10);

            if (terminate.get()) {
                return;
            }

            if (outdatedSessions.isEmpty()) {
                return;
            }

            LOG.info("Found some outdated allocator sessions, try to delete them");
            for (var session : outdatedSessions) {
                if (terminate.get()) {
                    return;
                }
                deleteAllocatorSession(session);
            }
        } catch (Exception e) {
            LOG.error("Got error during GC {} task", gcId, e);
        } finally {
            running.set(false);
        }
    }

    @Override
    public boolean cancel() {
        terminate.set(true);
        while (running.get()) {
            LockSupport.parkNanos(10_000);
        }
        return super.cancel();
    }

    private void deleteAllocatorSession(WorkflowDao.OutdatedAllocatorSession session) {
        LOG.info("About to delete outdated allocator session {}", session);

        var op = Operation.create(
            gcId,
            "Delete outdated allocator session " + session,
            null,
            null,
            null);

        try (var tx = TransactionHandle.create(operationRunnersFactory.storage())) {
            operationRunnersFactory.opDao().create(op, tx);
            var ok = operationRunnersFactory.wfDao().cleanAllocatorSessionId(
                session.userId(), session.wfName(), session.allocSessionId(), tx);
            if (ok) {
                operationRunnersFactory.deleteAllocSessionOpsDao().create(
                    op.id(), session.allocSessionId(), operationRunnersFactory.serviceConfig().getInstanceId(), tx);
                tx.commit();
            } else {
                return;
            }
        } catch (SQLException e) {
            LOG.warn("Cannot start delete allocator session {} operation: {}", session, e.getMessage());
            return;
        }

        if (interceptor != null) {
            interceptor.notifyStartDeleteAllocatorSessionOp(op.id());
        }

        var action = operationRunnersFactory.createDeleteAllocatorSessionOpRunner(
            op.id(), op.description(), null, session.allocSessionId(), null);
        operationRunnersFactory.executor().startNew(action);
    }
}
