//package ai.lzy.graph.test;
//
//import ai.lzy.graph.GraphExecutorApi;
//import ai.lzy.graph.LGE;
//import ai.lzy.graph.test.mocks.WorkerServiceMock;
//import ai.lzy.longrunning.OperationsService;
//import ai.lzy.v1.longrunning.LongRunning;
//import io.grpc.stub.StreamObserver;
//import io.micronaut.context.ApplicationContext;
//import io.prometheus.client.CollectorRegistry;
//import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
//import io.zonky.test.db.postgres.junit.PreparedDbRule;
//import org.junit.*;
//import org.junit.rules.Timeout;
//
//import java.util.List;
//import java.util.function.Consumer;
//
//import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;
//
//public class GraphExecutorTest {
//    @Rule
//    public Timeout globalTimeout = Timeout.seconds(100);
//    @Rule
//    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});
//
//    private ApplicationContext context;
//    private GraphExecutorApi api;
//    private OperationsService opService;
//
//    @Before
//    public void setUp() {
//        CollectorRegistry.defaultRegistry.clear();
//        context = ApplicationContext.run(preparePostgresConfig("graph-executor-2", db.getConnectionInfo()), "test-mock");
//        api = context.getBean(GraphExecutorApi.class);
//        opService = context.getBean(OperationsService.class);
//    }
//
//    @After
//    public void after() {
//        WorkerServiceMock.onAllocate = (a) -> {};
//        WorkerServiceMock.onExecute = (a) -> true;
//        context.stop();
//    }
//
//    @Test
//    public void simpleTest() throws Exception {
//        var request = LGE.ExecuteGraphRequest.newBuilder()
//            .setExecutionId("1")
//            .setWorkflowName("workflow1")
//            .setUserId("2")
//            .addAllTasks(List.of(
//                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
//                    .setId("task-1")
//                    .build()
//            ))
//            .build();
//        LongRunning.Operation op = getOp(s -> api.execute(request, s));
//        Assert.assertNotNull(op);
//
//        var result = getResult(op.getId());
//        while (result == null) {
//            Thread.sleep(10);
//            result = getResult(op.getId());
//        }
//
//        Assert.assertEquals("1", result.getWorkflowId());
//        Assert.assertTrue(result.hasCompleted());
//    }
//
//    @Test
//    public void errorOnValidationTest() {
//        var request = LGE.ExecuteGraphRequest.newBuilder()
//            .setExecutionId("1")
//            .setWorkflowName("workflow1")
//            .setUserId("2")
//            .build();
//        Assert.assertThrows(Exception.class, () -> getOp(s -> api.execute(request, s)));
//    }
//
//    @Test
//    public void errorOnAllocation() throws Exception {
//        WorkerServiceMock.onAllocate = (a) -> { throw new RuntimeException("Test"); };
//
//        var request = LGE.ExecuteGraphRequest.newBuilder()
//            .setExecutionId("1")
//            .setWorkflowName("workflow1")
//            .setUserId("2")
//            .addAllTasks(List.of(
//                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
//                    .setId("task-1")
//                    .build()
//            ))
//            .build();
//        LongRunning.Operation op = getOp(s -> api.execute(request, s));
//        Assert.assertNotNull(op);
//
//        var result = getResult(op.getId());
//        while (result == null) {
//            Thread.sleep(10);
//            result = getResult(op.getId());
//        }
//
//        Assert.assertEquals("1", result.getWorkflowId());
//        Assert.assertTrue(result.hasFailed());
//    }
//
//    @Test
//    public void errorOnExecute() throws Exception {
//        WorkerServiceMock.onExecute = (a) -> false;
//
//        var request = LGE.ExecuteGraphRequest.newBuilder()
//            .setExecutionId("1")
//            .setWorkflowName("workflow1")
//            .setUserId("2")
//            .addAllTasks(List.of(
//                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
//                    .setId("task-1")
//                    .build()
//            ))
//            .build();
//        LongRunning.Operation op = getOp(s -> api.execute(request, s));
//        Assert.assertNotNull(op);
//
//        var result = getResult(op.getId());
//        while (result == null) {
//            Thread.sleep(10);
//            result = getResult(op.getId());
//        }
//
//        Assert.assertEquals("1", result.getWorkflowId());
//        Assert.assertTrue(result.hasFailed());
//        var failed = result.getFailed();
//        Assert.assertEquals("task-1", failed.getFailedTaskId());
//    }
//
//    @Test
//    @Ignore
//    public void errorOnExecuteWith2Tasks() throws Exception {
//        WorkerServiceMock.onExecute = (a) -> a.getTaskId().equals("task-1");
//
//        var request = LGE.ExecuteGraphRequest.newBuilder()
//            .setExecutionId("1")
//            .setWorkflowName("workflow1")
//            .setUserId("2")
//            .addAllTasks(List.of(
//                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
//                    .setId("task-1")
//                    .build(),
//                LGE.ExecuteGraphRequest.TaskDesc.newBuilder()
//                    .setId("task-2")
//                    .build()
//            ))
//            .build();
//        LongRunning.Operation op = getOp(s -> api.execute(request, s));
//        Assert.assertNotNull(op);
//
//        var result = getResult(op.getId());
//        while (result == null) {
//            Thread.sleep(10);
//            result = getResult(op.getId());
//        }
//
//        Assert.assertEquals("1", result.getWorkflowId());
//        Assert.assertTrue(result.hasFailed());
//        var failed = result.getFailed();
//        Assert.assertEquals("task-2", failed.getFailedTaskId());
//    }
//
//    public LGE.ExecuteGraphResponse getResult(String opId) {
//        try {
//            var res = getOp(s -> opService.get(LongRunning.GetOperationRequest.newBuilder()
//                .setOperationId(opId)
//                .build(), s));
//            if (!res.getDone()) {
//                return null;
//            }
//            if (res.hasError()) {
//                return res.getMetadata().unpack(LGE.ExecuteGraphMetadata.class);
//            }
//            return res.getResponse().unpack(LGE.ExecuteGraphResponse.class);
//        } catch (Exception e) {
//            return null;
//        }
//    }
//
//    public LongRunning.Operation getOp(Consumer<StreamObserver<LongRunning.Operation>> func)
//        throws Exception
//    {
//        final LongRunning.Operation[] op = new LongRunning.Operation[1];
//        final Throwable[] ex = new Throwable[1];
//        func.accept(new StreamObserver<>() {
//            @Override
//            public void onNext(LongRunning.Operation operation) {
//                op[0] = operation;
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                ex[0] = throwable;
//            }
//
//            @Override
//            public void onCompleted() {
//
//            }
//        });
//
//        if (ex[0] != null) {
//            throw new Exception(ex[0]);
//        }
//
//        return op[0];
//    }
//}
