package ai.lzy.graph.services.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.model.DirectedGraph;
import ai.lzy.graph.model.Graph;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.services.GraphService;
import ai.lzy.graph.services.TaskService;
import ai.lzy.longrunning.dao.OperationDao;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class GraphServiceImpl implements GraphService {
    private final TaskService taskService;
    private final GraphDao graphDao;
    private final OperationDao operationDao;
    private final Map<String, Graph> graphs = new ConcurrentHashMap<>();

    @Inject
    public GraphServiceImpl(ServiceConfig config, TaskService taskService, GraphDao graphDao,
                            @Named("GraphExecutorOperationDao") OperationDao operationDao) {
        this.taskService = taskService;
        this.graphDao = graphDao;
        this.operationDao = operationDao;

        restoreGraphs(config.getInstanceId());
    }

    @Override
    public DirectedGraph buildGraph(GraphExecutorApi2.GraphExecuteRequest request) {
        return null;
    }

    @Override
    public boolean validateGraph(DirectedGraph graph) {
        return false;
    }

    @Override
    public void createTasks(DirectedGraph graph) {

    }

    @Override
    public void handleTaskCompleted(Task task) {

    }

    private void restoreGraphs(String instanceId) {

    }
}