package ai.lzy.graph.services.impl;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.model.DirectedGraph;
import ai.lzy.graph.model.Graph;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.services.GraphService;
import ai.lzy.graph.services.TaskService;
import ai.lzy.longrunning.dao.OperationDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class GraphServiceImpl implements GraphService {
    private static final Logger LOG = LogManager.getLogger(GraphServiceImpl.class);

    private final TaskService taskService;
    private final GraphDao graphDao;
    private final OperationDao operationDao;
    private final Map<String, Graph> graphs = new ConcurrentHashMap<>();

    @Inject
    public GraphServiceImpl(ServiceConfig config, TaskService taskService, GraphDao graphDao,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao)
    {
        this.taskService = taskService;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        restoreGraphs(config.getInstanceId());
    }

    @Override
    @Nullable
    public DirectedGraph buildGraph(GraphExecutorApi2.GraphExecuteRequest request) {
        return null;
    }

    @Override
    public void validateGraph(@Nullable DirectedGraph graph) {
    }

    @Override
    public void createTasks(DirectedGraph graph) {

    }

    @Override
    public void handleTaskCompleted(Task task) {

    }

    private void restoreGraphs(String instanceId) {
        try {
            withRetries(LOG, () -> {
                List<Graph> graphList = graphDao.getByInstance(instanceId);
                graphList.forEach(graph -> graphs.put(graph.id(), graph));
            });
        } catch (Exception e) {
            LOG.error("Cannot restore graphs for instance {}", instanceId);
            throw new RuntimeException(e);
        }
    }
}
