package ai.lzy.graph.test;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.graph.LGE;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.GraphState;
import ai.lzy.graph.model.TaskState;
import ai.lzy.graph.services.GraphService;
import ai.lzy.graph.services.TaskService;
import ai.lzy.graph.services.impl.GraphServiceImpl;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class GraphServiceTest {

    private final ServiceConfig config = Mockito.mock(ServiceConfig.class);
    private final TaskService taskService = Mockito.mock(TaskService.class);
    private final GraphDao graphDao = Mockito.mock(GraphDao.class);
    private final OperationDao operationDao = Mockito.mock(OperationDao.class);
    private final TaskDao taskDao = Mockito.mock(TaskDao.class);
    private final GraphExecutorDataSource storage = Mockito.mock(GraphExecutorDataSource.class);
    private final IdGenerator idGenerator = new RandomIdGenerator();
    private GraphService graphService;

    private final ArgumentCaptor<GraphState> graphCaptor = ArgumentCaptor.forClass(GraphState.class);
    private final ArgumentCaptor<List<TaskState>> tasksCaptor = ArgumentCaptor.forClass(List.class);

    @Before
    public void setUp() {
        graphService = new GraphServiceImpl(config, taskService, graphDao, operationDao, taskDao, storage, idGenerator);
    }

    @Test
    public void simpleTest() throws Exception {
        var request = LGE.ExecuteGraphRequest.newBuilder()
            .setUserId("2")
            .setWorkflowName("workflow1")
            .setExecutionId("1")
            .setAllocatorSessionId("sid1")
            .addAllTasks(List.of(
                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
                    .setId("task-1")
                    .build()))
            .build();
        var op = Operation.create(
            request.getUserId(),
            "Execute graph of execution: executionId='%s'".formatted(request.getExecutionId()),
            null, null, null);

        graphService.runGraph(request, op);
        Mockito.verify(operationDao).create(any(), any());
        Mockito.verify(graphDao).create(graphCaptor.capture(), any());

        GraphState graph = graphCaptor.getValue();
        assertEquals("1", graph.executionId());
        assertEquals("2", graph.userId());
        assertEquals("workflow1", graph.workflowName());

        Mockito.verify(taskDao).createTasks(any(), any());
        Mockito.verify(taskService).addTasks(tasksCaptor.capture());
        List<TaskState> tasks = tasksCaptor.getValue();
        assertEquals(1, tasks.size());
        assertEquals("task-1", tasks.get(0).id());
    }
}
