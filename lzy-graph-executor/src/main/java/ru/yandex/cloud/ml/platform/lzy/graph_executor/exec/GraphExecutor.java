package ru.yandex.cloud.ml.platform.lzy.graph_executor.exec;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.GraphExecutorApi;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;

@Singleton
public class GraphExecutor extends Thread{

    private final GraphExecutionDao dao;
    private final GraphProcessor processor;
    private final ExecutorService executor;
    private final int processingPeriodMillis;
    private final int batchSize;
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    private static final Logger LOG = LogManager.getLogger(GraphExecutorApi.class);


    @Inject
    public GraphExecutor(GraphExecutionDao dao, GraphProcessor processor, ServiceConfig config) {
        this.dao = dao;
        this.processor = processor;
        this.executor = Executors.newFixedThreadPool(config.threadPoolSize());
        processingPeriodMillis = config.processingPeriodMillis();
        batchSize = config.batchSize();
    }


    @Override
    public void run() {
        while (true) {
            try {
                boolean isStopped = stopLatch.await(processingPeriodMillis, TimeUnit.MILLISECONDS);
                if (isStopped) {
                    return;
                }
                dao.updateListAtomic(
                    Set.of(
                        GraphExecutionState.Status.WAITING,
                        GraphExecutionState.Status.EXECUTING,
                        GraphExecutionState.Status.SCHEDULED_TO_FAIL
                    ),
                    state -> executor.submit(() -> processor.exec(state)),
                    batchSize
                );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (GraphExecutionDao.GraphDaoException e) {
                LOG.error("Cannot update state for some graph, waiting for next period to execute");
            }
        }
    }

    public void gracefulStop() {
        stopLatch.countDown();
    }
}
