package ai.lzy.service.gc;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.service.operations.OperationRunnersFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.TimerTask;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);

    private final String gcId;
    private final OperationRunnersFactory operationRunnersFactory;

    public GarbageCollectorTask(String id, OperationRunnersFactory operationRunnersFactory) {
        this.gcId = id;
        this.operationRunnersFactory = operationRunnersFactory;
    }

    @Override
    public void run() {
        try {
            var outdatedSessions = operationRunnersFactory.wfDao().listOutdatedAllocatorSessions(10);

            if (outdatedSessions.isEmpty()) {
                return;
            }

            LOG.info("Found some outdated allocator sessions, try to delete them");
            for (var session : outdatedSessions) {
                deleteAllocatorSession(session);
            }
        } catch (Exception e) {
            LOG.error("Got error during GC {} task", gcId, e);
        }
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

        var action = operationRunnersFactory.createDeleteAllocatorSessionOpRunner(
            op.id(), op.description(), null, session.allocSessionId(), null);
        operationRunnersFactory.executor().startNew(action);
    }
}
