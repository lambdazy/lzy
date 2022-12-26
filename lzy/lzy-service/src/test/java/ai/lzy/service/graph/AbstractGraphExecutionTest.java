package ai.lzy.service.graph;

import ai.lzy.graph.test.GraphExecutorMock;
import ai.lzy.service.BaseTest;
import ai.lzy.test.IdempotencyUtils.TestScenario;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Graph;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public abstract class AbstractGraphExecutionTest extends BaseTest {
    private Server graphExecutorServer;

    @Override
    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();

        var graphExecutorAddress = HostAndPort.fromString(config.getGraphExecutorAddress());

        graphExecutorMock = new GraphExecutorMock();
        graphExecutorServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(graphExecutorAddress.getHost(), graphExecutorAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(graphExecutorMock, authInterceptor))
            .build();
        graphExecutorServer.start();
    }

    @Override
    @After
    public void tearDown() throws java.sql.SQLException, InterruptedException {
        super.tearDown();
        graphExecutorServer.shutdown();
    }

    static LWFS.StartExecutionResponse startExecution(LzyWorkflowServiceBlockingStub client) {
        var workflowName = "workflow_1";
        return client.startExecution(LWFS.StartExecutionRequest.newBuilder()
            .setWorkflowName(workflowName).build());
    }

    Graph buildSimpleGraph(LMS3.S3Locator s3locator) {
        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        return Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addAllOperations(operations)
            .build();
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, String> executeSimpleGraphScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);
                LWF.Graph graph = buildSimpleGraph(workflow.getInternalSnapshotStorage());
                var executionId = workflow.getExecutionId();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) ->
                stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setExecutionId(input.getKey())
                    .setGraph(input.getValue())
                    .build()).getGraphId(),
            graphId -> assertFalse(graphId.isBlank()));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> emptyGraphScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);

                var executionId = workflow.getExecutionId();
                var graph = LWF.Graph.newBuilder()
                    .setName("simple-graph")
                    .build();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) ->
                assertThrows(StatusRuntimeException.class, () -> {
                    //noinspection ResultOfMethodCallIgnored
                    stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(input.getKey())
                        .setGraph(input.getValue())
                        .build());
                }).getStatus().getCode(),
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> duplicatedSlotScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);
                LMS3.S3Locator s3locator = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints strings to variables")
                        .setCommand(
                            "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a && echo 'hello' > /tmp/lzy_worker_1/b")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator)))
                        .setPoolSpecName("s")
                        .build();

                var executionId = workflow.getExecutionId();
                var graph = LWF.Graph.newBuilder()
                    .setName("simple-graph")
                    .addOperations(operation)
                    .build();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) ->
                assertThrows(StatusRuntimeException.class,
                    () -> {
                        //noinspection ResultOfMethodCallIgnored
                        stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                            .setExecutionId(input.getKey())
                            .setGraph(input.getValue())
                            .build());
                    }).getStatus().getCode(),
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> cyclicDataflowGraphScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);
                LMS3.S3Locator s3locator = workflow.getInternalSnapshotStorage();

                var operationsWithCycleDependency = List.of(
                    LWF.Operation.newBuilder()
                        .setName("first operation")
                        .setCommand("echo '42' > /tmp/lzy_worker_1/a && " +
                            "/tmp/lzy_worker_1/sbin/cat /tmp/lzy_worker_1/c > /tmp/lzy_worker_1/b")
                        .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/c")
                            .setStorageUri(buildSlotUri("snapshot_c_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
                            .build())
                        .setPoolSpecName("s")
                        .build(),
                    LWF.Operation.newBuilder()
                        .setName("second operation")
                        .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/d &&" +
                            " /tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/d > /tmp/lzy_worker_2/c")
                        .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/d")
                            .setStorageUri(buildSlotUri("snapshot_d_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/c")
                            .setStorageUri(buildSlotUri("snapshot_c_1", s3locator))
                            .build())
                        .setPoolSpecName("s")
                        .build());

                var executionId = workflow.getExecutionId();
                var graph = LWF.Graph.newBuilder()
                    .setName("simple-graph")
                    .addAllOperations(operationsWithCycleDependency)
                    .build();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) ->
                assertThrows(StatusRuntimeException.class,
                    () -> {
                        //noinspection ResultOfMethodCallIgnored
                        stub.executeGraph(
                            LWFS.ExecuteGraphRequest.newBuilder()
                                .setExecutionId(input.getKey())
                                .setGraph(input.getValue())
                                .build());
                    }).getStatus().getCode(),
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> unknownInputSlotUriScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);
                LMS3.S3Locator s3locator = workflow.getInternalSnapshotStorage();

                var unknownStorageUri = buildSlotUri("snapshot_a_1", s3locator);

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints strings to variable")
                        .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                        .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/a")
                            .setStorageUri(unknownStorageUri)
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/b")
                            .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
                            .build())
                        .setPoolSpecName("s")
                        .build();

                var graph = LWF.Graph.newBuilder()
                    .setName("simple-graph-2")
                    .addOperations(operation)
                    .build();

                var executionId = workflow.getExecutionId();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) ->
                assertThrows(StatusRuntimeException.class,
                    () -> {
                        //noinspection ResultOfMethodCallIgnored
                        stub.executeGraph(
                            LWFS.ExecuteGraphRequest.newBuilder()
                                .setExecutionId(input.getKey())
                                .setGraph(input.getValue())
                                .build());
                    }).getStatus().getCode(),
            errorCode -> assertEquals(Status.NOT_FOUND.getCode(), errorCode));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> withoutSuitableZoneScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                var workflow = startExecution(stub);
                LMS3.S3Locator s3locator = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints string to variable")
                        .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                            .build())
                        .setPoolSpecName("m")
                        .build();

                var executionId = workflow.getExecutionId();
                var graph = LWF.Graph.newBuilder()
                    .setName("simple-graph")
                    .addOperations(operation)
                    .build();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) -> assertThrows(StatusRuntimeException.class,
                () -> {
                    //noinspection ResultOfMethodCallIgnored
                    stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(input.getKey())
                        .setGraph(input.getValue())
                        .build());
                }).getStatus().getCode(),
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode));
    }

    TestScenario<LzyWorkflowServiceBlockingStub, Map.Entry<String, Graph>, Status.Code> nonSuitableZoneScenario() {
        return new TestScenario<>(authorizedWorkflowClient,
            stub -> {
                LWFS.StartExecutionResponse workflow = startExecution(stub);
                LMS3.S3Locator s3locator = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints strings to variables")
                        .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a && echo 'hi' > /tmp/lzy_worker_1/b")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_b_1", s3locator)))
                        .setPoolSpecName("l")
                        .build();

                var executionId = workflow.getExecutionId();
                var graph = Graph.newBuilder()
                    .setName("simple-graph")
                    .setZone("ru-central1-a")
                    .addOperations(operation)
                    .build();

                return new AbstractMap.SimpleEntry<>(executionId, graph);
            },
            /* action */
            (stub, input) -> assertThrows(StatusRuntimeException.class,
                () -> {
                    //noinspection ResultOfMethodCallIgnored
                    stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(input.getKey())
                        .setGraph(input.getValue())
                        .build());
                }).getStatus().getCode(),
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode));
    }
}
