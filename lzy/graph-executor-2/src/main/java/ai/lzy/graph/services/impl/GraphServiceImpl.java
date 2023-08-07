package ai.lzy.graph.services.impl;

import ai.lzy.common.IdGenerator;
import ai.lzy.graph.LGE;
import ai.lzy.graph.algo.Algorithms;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.GraphState;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.GraphService;
import ai.lzy.graph.services.TaskService;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoConverter.toProto;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@Singleton
public class GraphServiceImpl implements GraphService {
    private static final Logger LOG = LogManager.getLogger(GraphServiceImpl.class);

    private final TaskService taskService;
    private final GraphDao graphDao;
    private final TaskDao taskDao;
    private final OperationDao operationDao;
    private final GraphExecutorDataSource storage;
    private final IdGenerator idGenerator;
    private final Map<String, GraphState> graphs = new ConcurrentHashMap<>();

    @Inject
    public GraphServiceImpl(ServiceConfig config, TaskService taskService, GraphDao graphDao,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao,
                            TaskDao taskDao, GraphExecutorDataSource storage,
                            @Named("GraphExecutorIdGenerator") IdGenerator idGenerator)
    {
        this.taskService = taskService;
        this.graphDao = graphDao;
        this.operationDao = operationDao;
        this.taskDao = taskDao;
        this.storage = storage;
        this.idGenerator = idGenerator;

        taskService.init(this::handleTaskStatusChanged);
        restoreGraphs(config.getInstanceId());
    }

    @Override
    public void runGraph(LGE.ExecuteGraphRequest request, Operation op) throws Exception {
        var graphId = idGenerator.generate("gr-");
        var graphState = GraphState.fromProto(request, graphId, op.id());

        var channels = request.getChannelsList().stream()
            .map(LGE.ExecuteGraphRequest.ChannelDesc::getId)
            .toList();

        var tasks = request.getTasksList().stream()
            .map(task -> TaskState.fromProto(task, graphState))
            .toList();

        Algorithms.buildTaskDependents(graphState, tasks, channels);

        try (var guard = graphState.bind()) {
            graphState.initTasks(tasks.stream().map(TaskState::id).collect(toSet()));

            var opMeta = graphState.toMetaProto(taskService::getTaskStatus);
            op.modifyMeta(opMeta);

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    operationDao.create(op, tx);
                    graphDao.create(graphState, tx);
                    taskDao.createTasks(tasks, tx);
                    tx.commit();
                }
            });
        }

        LOG.info("Register new graph: {}", graphState);

        graphs.put(graphId, graphState);
        taskService.addTasks(tasks);
    }

    private void handleTaskStatusChanged(TaskState task) {
        var graph = graphs.get(task.graphId());

        if (graph == null) {
            LOG.error("Got task {} status change from unknown graph {}", task.id(), task.graphId());
            return;
        }

        try (var guard = graph.bind()) {
            if (graph.status().finished()) {
                LOG.warn("Got task {} status change from {} graph {}", task.id(), graph.status(), task.graphId());
                return;
            }

            switch (task.status()) {
                case WAITING_ALLOCATION, ALLOCATING, EXECUTING -> graph.tryExecute(task.id());
                case COMPLETED -> graph.tryComplete(task.id());
                case FAILED -> graph.tryFail(task.id(), task.name(), task.errorDescription());
            }

            var opMeta = Any.pack(graph.toMetaProto(taskService::getTaskStatus));

            try {
                withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        switch (graph.status()) {
                            case WAITING, EXECUTING -> {
                                operationDao.updateMeta(graph.operationId(), opMeta, tx);
                            }
                            case COMPLETED -> {
                                var opResp = Any.pack(requireNonNull(graph.toResponseProto()));
                                operationDao.complete(graph.operationId(), opMeta, opResp, tx);
                            }
                            case FAILED -> {
                                var status = io.grpc.Status.INTERNAL.withDescription(graph.errorDescription());
                                operationDao.updateMeta(graph.operationId(), opMeta, tx);
                                operationDao.failOperation(graph.operationId(), toProto(status), tx, LOG);
                            }
                        }

                        graphDao.update(graph, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                LOG.error("Cannot update graph {} status", graph.id());
                throw new RuntimeException(e);
            }
        }
    }

    private void restoreGraphs(String instanceId) {
        try {
            var graphsList = graphDao.getActiveByInstance(instanceId);
            LOG.info("Restore {} running graphs on GraphExecutor {}", graphs.size(), instanceId);

            for (var graphState : graphsList) {
                /*
                var tasks = taskDao.getTasksByGraph(graphState.id());

                tasks.forEach(task -> {
                    graphState.tasks()
                        .computeIfAbsent(GraphState.Status.fromTaskStatus(task.status()), x -> new ArrayList<>())
                        .add(task.id());

                    // TODO: dependencies
                });

                var activeTasks = tasks.stream()
                    .filter(task -> !task.status().finished())
                    .collect(toList());
                 */

                graphs.put(graphState.id(), graphState);
                //taskService.addTasks(activeTasks);
            }
        } catch (Exception e) {
            LOG.error("Cannot restore graphs for instance {}", instanceId, e);
            throw new RuntimeException(e);
        }
    }
}
