package ru.yandex.cloud.ml.platform.lzy.graph.queue;

import static java.util.concurrent.TimeUnit.SECONDS;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.yandex.cloud.ml.platform.lzy.graph.config.ServiceConfig;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao.GraphDaoException;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.GraphProcessor;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState.Status;

@Singleton
public class QueueManager extends Thread {
    private static final ThreadGroup EXECUTORS_TG = new ThreadGroup("graph-executors");
    private static final long PERIOD_SECONDS = 1;
    private static final long TERMINATION_TIMEOUT_SECONDS = 10;
    private static final Logger LOG = LogManager.getLogger(QueueManager.class);

    private final BlockingQueue<GraphExecutionKey> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private final GraphProcessor processor;
    private final GraphExecutionDao dao;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final Map<GraphExecutionKey, String> stoppingGraphs = new ConcurrentHashMap<>();
    private final ServiceConfig config;

    @Inject
    public QueueManager(GraphProcessor processor, GraphExecutionDao dao, ServiceConfig config) {
        super("queue-manager-thread");
        this.processor = processor;
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(config.executorsCount(), new ThreadFactory() {
            private int count = 0;

            @Override
            public synchronized Thread newThread(@NotNull Runnable r) {
                return new Thread(EXECUTORS_TG, r, "graph-executor-" + (++count));
            }
        });
        this.config = config;
        try {
            restore();
        } catch (GraphDaoException e) {
            LOG.error("Error while restoring graph executor", e);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (stopping.get()) {
                    shutdown();
                    return;
                }
                GraphExecutionKey key = queue.poll(PERIOD_SECONDS, SECONDS);
                if (key == null) {
                    continue;
                }

                executor.submit(() -> process(key));

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void gracefulShutdown() {
        stopping.set(true);
    }

    public GraphExecutionState startGraph(String workflowId, GraphDescription graph) throws GraphDaoException {
        try {
            final GraphExecutionState state = dao.create(workflowId, graph);
            queue.put(new GraphExecutionKey(state.workflowId(), state.id()));
            return state;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    public GraphExecutionState stopGraph(String workflowId, String graphId,
                                         String description) throws GraphDaoException {
        final GraphExecutionState state = dao.get(workflowId, graphId);
        if (state == null) {
            return null;
        }
        if (Set.of(Status.FAILED, Status.COMPLETED).contains(state.status())) {
            return state;
        }
        stoppingGraphs.put(new GraphExecutionKey(workflowId, graphId), description);
        return state;
    }

    private void shutdown() throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
            executor.shutdownNow();
        }
    }

    private void process(@NotNull GraphExecutionKey stateKey) {
        final GraphExecutionState state;
        try {
            state = dao.acquire(stateKey.workflowId(), stateKey.graphId(),
                config.executionStepTimeoutSecs(), ChronoUnit.SECONDS);
            if (state == null) {
                return;
            }
            final GraphExecutionState newState;
            if (stoppingGraphs.containsKey(stateKey)) {
                newState = processor.stop(state, stoppingGraphs.remove(stateKey));
            } else {
                newState = processor.exec(state);
            }
            dao.free(newState);
            if (!Set.of(Status.FAILED, Status.COMPLETED).contains(newState.status())) {
                queue.put(stateKey);
            }
        } catch (GraphDaoException | InterruptedException e) {
            throw new RuntimeException(
                String.format(
                    "Cannot update graph <%s> with workflow <%s>",
                    stateKey.graphId(), stateKey.workflowId()),
                e
            );
        }
    }

    private void restore() throws GraphDaoException {
        final List<GraphExecutionState> states = dao.filter(Status.EXECUTING);
        states.addAll(dao.filter(Status.WAITING));
        states.forEach(t -> {
            try {
                final GraphExecutionState s = dao.acquire(t.workflowId(), t.id(), 1, ChronoUnit.SECONDS);
                if (s == null) {
                    return;
                }
                queue.put(new GraphExecutionKey(s.workflowId(), s.id()));
                dao.free(s);
            } catch (GraphDaoException | InterruptedException e) {
                final GraphExecutionKey key = new GraphExecutionKey(t.workflowId(), t.id());
                stoppingGraphs.put(key, "Error while restoring graphExecution");
                try {
                    queue.put(key);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    private record GraphExecutionKey(String workflowId, String graphId) {}
}
