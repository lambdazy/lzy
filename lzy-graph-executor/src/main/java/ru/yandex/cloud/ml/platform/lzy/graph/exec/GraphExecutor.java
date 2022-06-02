package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph.GraphExecutorApi;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

public class GraphExecutor extends Thread {

    private final GraphExecutionDao dao;
    private final GraphProcessor processor;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);

    public GraphExecutor(GraphExecutionDao dao, GraphProcessor processor) {
        this.dao = dao;
        this.processor = processor;
    }


    @Override
    public void run() {
        while (!stopped.get()) {
            try {
                dao.updateAtomic(
                    Set.of(
                        GraphExecutionState.Status.WAITING,
                        GraphExecutionState.Status.EXECUTING,
                        GraphExecutionState.Status.SCHEDULED_TO_FAIL
                    ),
                    processor::exec
                );
            } catch (GraphExecutionDao.GraphDaoException e) {
                LOG.error("Cannot update state for some graph, waiting for next period to execute");
            }
        }
    }

    public void gracefulStop() {
        stopped.set(true);
    }
}
