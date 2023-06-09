/*
package ai.lzy.service.gc;

import ai.lzy.service.dao.ExecutionDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TimerTask;

public class GarbageCollectorTask extends TimerTask {
    private static final Logger LOG = LogManager.getLogger(GarbageCollectorTask.class);
    private final String id;
    private final ExecutionDao executionDao;

    private final CleanExecutionCompanion cleanExecutionCompanion;

    public GarbageCollectorTask(String id, ExecutionDao executionDao, CleanExecutionCompanion cleanExecutionCompanion) {
        this.id = id;
        this.executionDao = executionDao;
        this.cleanExecutionCompanion = cleanExecutionCompanion;
    }

    @Override
    public void run() {
        try {
            String expiredExecution = executionDao.getExpiredExecution();
            while (expiredExecution != null) {
                cleanExecution(expiredExecution);
                expiredExecution = executionDao.getExpiredExecution();
            }
        } catch (Exception e) {
            LOG.error("Got error during GC {} task", id, e);
        }
    }

    private void cleanExecution(String executionId) {
        LOG.info("Execution {} is expired, GC {}", executionId, id);
        cleanExecutionCompanion.cleanExecution(executionId);
    }
}
*/
