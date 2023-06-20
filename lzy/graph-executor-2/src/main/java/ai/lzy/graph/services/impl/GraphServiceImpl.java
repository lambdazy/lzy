package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
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
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class GraphServiceImpl implements GraphService {
    private static final Logger LOG = LogManager.getLogger(GraphServiceImpl.class);

    private final TaskService taskService;
    private final GraphDao graphDao;
    private final TaskDao taskDao;
    private final OperationDao operationDao;
    private final GraphExecutorDataSource storage;
    private final Map<String, GraphState> graphs = new ConcurrentHashMap<>();

    @Inject
    public GraphServiceImpl(ServiceConfig config, TaskService taskService, GraphDao graphDao,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao,
                            TaskDao taskDao, GraphExecutorDataSource storage)
    {
        this.taskService = taskService;
        this.graphDao = graphDao;
        this.operationDao = operationDao;
        this.taskDao = taskDao;
        this.storage = storage;

        taskService.init(this::handleTaskCompleted);
        restoreGraphs(config.getInstanceId());
    }

    @Override
    public void buildGraph(GraphExecutorApi2.GraphExecuteRequest request, Operation op) throws Exception {
        final String graphId = UUID.randomUUID().toString();
        final GraphState graph = GraphState.fromProto(request, graphId, op.id());
        final List<String> channels = request.getChannelsList()
            .stream()
            .map(GraphExecutorApi2.GraphExecuteRequest.ChannelDesc::getId)
            .toList();
        List<TaskState> tasks = request.getTasksList().stream()
            .map(descr -> TaskState.fromProto(descr, graph))
            .toList();

        Algorithms.buildTaskDependents(graph, tasks, channels);

        try (var tx = TransactionHandle.create(storage)) {
            withRetries(LOG, () -> operationDao.create(op, tx));
            withRetries(LOG, () -> graphDao.create(graph, tx));
            withRetries(LOG, () -> taskDao.createTasks(tasks, tx));
            tx.commit();
        }
        graphs.put(graphId, graph);
        taskService.addTasks(tasks);
    }

    @Override
    public void handleTaskCompleted(TaskState task) {

    }

    private void restoreGraphs(String instanceId) {
        try {
            withRetries(LOG, () -> {
                List<GraphState> graphList = graphDao.getByInstance(instanceId);
                graphList.forEach(graph -> graphs.put(graph.id(), graph));
            });
        } catch (Exception e) {
            LOG.error("Cannot restore graphs for instance {}", instanceId);
            throw new RuntimeException(e);
        }
    }
}
