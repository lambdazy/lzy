package ai.lzy.service.graph;

import ai.lzy.service.BaseTest;
import ai.lzy.test.IdempotencyUtils.TestScenario;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Graph;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class AbstractGraphExecutionTest extends BaseTest {
    static LWFS.StartWorkflowResponse startExecution(LzyWorkflowServiceBlockingStub client) {
        var workflowName = "workflow_1";
        return client.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build());
    }

    Graph buildSimpleGraph(LMST.StorageConfig storageConfig) {
        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);
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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);

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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);
                LMST.StorageConfig storageConfig = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints strings to variables")
                        .setCommand(
                            "echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a && echo 'hello' > /tmp/lzy_worker_1/b")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig)))
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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);
                LMST.StorageConfig storageConfig = workflow.getInternalSnapshotStorage();

                var operationsWithCycleDependency = List.of(
                    LWF.Operation.newBuilder()
                        .setName("first operation")
                        .setCommand("echo '42' > /tmp/lzy_worker_1/a && " +
                            "/tmp/lzy_worker_1/sbin/cat /tmp/lzy_worker_1/c > /tmp/lzy_worker_1/b")
                        .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/c")
                            .setStorageUri(buildSlotUri("snapshot_c_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                            .build())
                        .setPoolSpecName("s")
                        .build(),
                    LWF.Operation.newBuilder()
                        .setName("second operation")
                        .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/d &&" +
                            " /tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/d > /tmp/lzy_worker_2/c")
                        .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/d")
                            .setStorageUri(buildSlotUri("snapshot_d_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_2/c")
                            .setStorageUri(buildSlotUri("snapshot_c_1", storageConfig))
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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);
                LMST.StorageConfig storageConfig = workflow.getInternalSnapshotStorage();

                var unknownStorageUri = buildSlotUri("snapshot_a_1", storageConfig);

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
                            .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
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
                LMST.StorageConfig storageConfig = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints string to variable")
                        .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
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
                LWFS.StartWorkflowResponse workflow = startExecution(stub);
                LMST.StorageConfig storageConfig = workflow.getInternalSnapshotStorage();

                var operation =
                    LWF.Operation.newBuilder()
                        .setName("prints strings to variables")
                        .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a && echo 'hi' > /tmp/lzy_worker_1/b")
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/a")
                            .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                            .build())
                        .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                            .setPath("/tmp/lzy_worker_1/b")
                            .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig)))
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
