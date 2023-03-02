package ai.lzy.graph.queue;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.db.QueueEventDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.exec.GraphProcessor;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.GraphExecutionState.Status;
import ai.lzy.graph.model.QueueEvent;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.util.grpc.GrpcHeaders;
import io.grpc.StatusException;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class QueueManager extends Thread {
    private static final ThreadGroup EXECUTORS_TG = new ThreadGroup("graph-executors");
    private static final long TERMINATION_TIMEOUT_SECONDS = 10;
    private static final Logger LOG = LogManager.getLogger(QueueManager.class);

    private final BlockingQueue<GraphExecutionKey> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private final GraphProcessor processor;
    private final GraphExecutorDataSource storage;
    private final GraphExecutionDao dao;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final Map<GraphExecutionKey, String> stoppingGraphs = new ConcurrentHashMap<>();
    private final QueueEventDao eventDao;

    private final OperationDao operationDao;

    @Inject
    public QueueManager(ServiceConfig config, GraphProcessor processor, GraphExecutorDataSource storage,
                        QueueEventDao eventDao, GraphExecutionDao dao,
                        @Named("GraphExecutorOperationDao") OperationDao operationDao)
    {
        super("queue-manager-thread");
        this.processor = processor;
        this.dao = dao;
        this.eventDao = eventDao;
        this.executor = Executors.newFixedThreadPool(config.getExecutorsCount(), new ThreadFactory() {
            private int count = 0;

            @Override
            public synchronized Thread newThread(@NotNull Runnable r) {
                return new Thread(EXECUTORS_TG, r, "graph-executor-" + (++count));
            }
        });
        this.storage = storage;
        this.operationDao = operationDao;
        try {
            restore();
        } catch (DaoException e) {
            LOG.error("Error while restoring graph executor", e);
        }
    }

    @Override
    public void run() {
        while (true) {
            if (stopping.get()) {
                shutdown();
                return;
            }
            try {
                eventDao.acquireWithLimit(128)
                    .forEach(this::processEventFromQueue);
            } catch (DaoException e) {
                LOG.error("Error while processing queue events", e);
            }
            try {
                GraphExecutionKey key = queue.take();
                if (GraphExecutionKey.isNoop(key)) {
                    continue;
                }
                executor.submit(() -> process(key));
            } catch (InterruptedException e) {
                LOG.debug("Thread is interrupted", e);
            }
        }
    }

    public void gracefulShutdown() {
        stopping.set(true);
        putIntoQueue(GraphExecutionKey.noop());
    }

    public GraphExecutionState startGraph(String workflowId, String workflowName, String userId, GraphDescription graph,
                                          @Nullable Operation operation) throws Exception
    {
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service stopping, please try later").asException();
        }

        final GraphExecutionState state;

        try (var tx = TransactionHandle.create(storage)) {
            if (operation != null) {
                withRetries(LOG, () -> operationDao.create(operation, tx));
            }

            state = withRetries(LOG, () -> dao.create(workflowId, workflowName, userId, graph, tx));
            withRetries(LOG, () -> eventDao.add(QueueEvent.Type.START, state.workflowId(), state.id(),
                "Starting graph", tx));

            tx.commit();
        }

        putIntoQueue(GraphExecutionKey.noop());

        return state;
    }

    public GraphExecutionState stopGraph(String workflowId, String graphId,
                                         String description) throws StatusException
    {
        if (stopping.get()) {
            throw io.grpc.Status.UNAVAILABLE.withDescription("Service stopping, please try later").asException();
        }
        final GraphExecutionState state;
        try {
            state = dao.get(workflowId, graphId);
            if (state == null) {
                throw io.grpc.Status.NOT_FOUND.withDescription("Graph not found").asException();
            }
            if (Set.of(Status.FAILED, Status.COMPLETED).contains(state.status())) {
                return state;
            }
            withRetries(LOG, () -> eventDao.add(QueueEvent.Type.STOP, state.workflowId(), state.id(),
                description, null));
        } catch (Exception e) {
            LOG.error("Error while adding graph {} stop event from workflow {}", graphId, workflowId, e);
            throw io.grpc.Status.INTERNAL.withDescription("Error while stopping graph").asException();
        }
        putIntoQueue(GraphExecutionKey.noop());
        return state;
    }

    private void shutdown() {
        executor.shutdown();
        LocalDateTime start = LocalDateTime.now();
        while (true) {
            try {
                if (!executor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                    executor.shutdownNow();
                }
                return;
            } catch (InterruptedException e) {
                if (start.plus(TERMINATION_TIMEOUT_SECONDS, ChronoUnit.SECONDS).isAfter(LocalDateTime.now())) {
                    executor.shutdownNow();
                    return;
                }
            }
        }

    }

    private void process(@NotNull GraphExecutionKey stateKey) {
        final GraphExecutionState state;
        try {
            state = dao.acquire(stateKey.workflowId(), stateKey.graphId());
        } catch (DaoException e) {
            LOG.error("Cannot acquire state", e);
            return;
        }
        try {
            if (state == null) {
                return;
            }
            final GraphExecutionState newState =
                GrpcHeaders.withContext()
                .withHeader(GrpcHeaders.USER_LOGS_HEADER_KEY, state.description().logsHeader())
                .run(() -> {
                    if (stoppingGraphs.containsKey(stateKey)) {
                        return processor.stop(state, stoppingGraphs.remove(stateKey));
                    } else {
                        return processor.exec(state);
                    }
                });

            dao.updateAndFree(newState);
            if (!Set.of(Status.FAILED, Status.COMPLETED).contains(newState.status())) {
                putIntoQueue(stateKey);
            } else {
                LOG.info("Processing of graph {} stopped", state.id());
            }
        } catch (Exception e) {
            LOG.error("Error while executing graph", e);
            try {
                dao.updateAndFree(state);  // Free state if it is acquired
                stopGraph(stateKey.workflowId(), stateKey.graphId(),
                    "Stopped because of exception in executor");
                putIntoQueue(stateKey);
            } catch (StatusException | DaoException ex) {
                LOG.error("Cannot stop graph <{}> from workflow <{}>",
                    stateKey.workflowId(), stateKey.graphId(), ex);
            }
            throw new RuntimeException(String.format("Cannot update graph <%s> with workflow <%s>",
                stateKey.graphId(), stateKey.workflowId()), e);
        }
    }

    private void restore() throws DaoException {
        final List<GraphExecutionState> states = dao.filter(Status.EXECUTING);
        states.addAll(dao.filter(Status.WAITING));
        states.forEach(t -> {
            try {
                final GraphExecutionState s = dao.acquire(t.workflowId(), t.id());
                if (s == null) {
                    return;
                }
                putIntoQueue(new GraphExecutionKey(s.workflowId(), s.id()));
                dao.updateAndFree(s);
            } catch (DaoException e) {
                final GraphExecutionKey key = new GraphExecutionKey(t.workflowId(), t.id());
                stoppingGraphs.put(key, "Error while restoring graphExecution");
                putIntoQueue(key);
            }
        });
        eventDao.removeAllAcquired();
    }

    private void putIntoQueue(GraphExecutionKey key) {
        boolean res = queue.offer(key);
        if (!res) {
            throw new RuntimeException("Cannot put graph into queue"); // Unreachable
        }
    }

    private void processEventFromQueue(QueueEvent event) {
        final GraphExecutionKey key = new GraphExecutionKey(event.workflowId(), event.graphId());
        switch (event.type()) {
            case START -> putIntoQueue(key);
            case STOP -> stoppingGraphs.put(key, event.description());
            default -> {
            }
        }
        try {
            eventDao.remove(event);
        } catch (DaoException e) {
            LOG.error("Error while removing event {} from queue", event, e);
        }
    }

    private record GraphExecutionKey(String workflowId, String graphId) {
        public static GraphExecutionKey noop() {
            return new GraphExecutionKey("noop", "noop");
        }

        public static boolean isNoop(GraphExecutionKey key) {
            return key.graphId().equals("noop") && key.workflowId().equals("noop");
        }
    }
}
