package ai.lzy.service.graph;

import ai.lzy.service.BaseTest;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Graph;
import ai.lzy.v1.workflow.LWFS;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

abstract class AbstractGraphExecutionTest extends BaseTest {
    LMST.StorageConfig storageConfig;

    @Override
    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        storageConfig = authorizedWorkflowClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
    }

    LWFS.StartWorkflowResponse startWorkflow(String name) {
        return startWorkflow(name, storageConfig);
    }

    Graph emptyGraph() {
        return Graph.newBuilder().setName("empty-graph").build();
    }

    Graph simpleGraph() {
        return simpleGraph(storageConfig);
    }

    Graph simpleGraph(LMST.StorageConfig storageConfig) {
        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        return Graph.newBuilder().setName("simple-graph").setZone("ru-central1-a").addAllOperations(operations).build();
    }

    Graph cyclicGraph() {
        return cyclicGraph(storageConfig);
    }

    Graph cyclicGraph(LMST.StorageConfig storageConfig) {
        var operationsWithCycleDependency = List.of(
            LWF.Operation.newBuilder()
                .setName("first operation")
                .setCommand("echo '42' > $LZY_MOUNT/a && " +
                    "$LZY_MOUNT/sbin/cat $LZY_MOUNT/c > $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/c")
                    .setStorageUri(buildSlotUri("snapshot_c_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second operation")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/d &&" +
                    " $LZY_MOUNT/sbin/cat $LZY_MOUNT/d > $LZY_MOUNT/c")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/d")
                    .setStorageUri(buildSlotUri("snapshot_d_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/c")
                    .setStorageUri(buildSlotUri("snapshot_c_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build());

        return Graph.newBuilder().setName("cyclic-graph").addAllOperations(operationsWithCycleDependency).build();
    }

    Graph nonSuitableZoneGraph() {
        return nonSuitableZoneGraph(storageConfig);
    }

    Graph nonSuitableZoneGraph(LMST.StorageConfig storageConfig) {
        var operation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variables")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a && echo 'hi' > $LZY_MOUNT/b")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig)))
                .setPoolSpecName("l")
                .build();

        return Graph.newBuilder().setName("pool-non-exists").setZone("ru-central1-a").addOperations(operation).build();
    }

    Graph invalidZoneGraph() {
        return invalidZoneGraph(storageConfig);
    }

    Graph invalidZoneGraph(LMST.StorageConfig storageConfig) {
        var operation =
            LWF.Operation.newBuilder()
                .setName("prints string to variable")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("not-existing-spec")
                .build();

        return Graph.newBuilder().setName("invalid-pool").addOperations(operation).build();
    }

    Graph unknownSlotUriGraph() {
        return unknownSlotUriGraph(storageConfig);
    }

    Graph unknownSlotUriGraph(LMST.StorageConfig storageConfig) {
        var unknownStorageUri = buildSlotUri("snapshot_a_1", storageConfig);

        var operation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variable")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(unknownStorageUri)
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build();

        return Graph.newBuilder().setName("unknown-slot-uri").addOperations(operation).build();
    }

    Graph withMissingOutputSlot() {
        return withMissingOutputSlot(storageConfig);
    }

    Graph withMissingOutputSlot(LMST.StorageConfig storageConfig) {
        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("operation-1")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("operation-2")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        return Graph.newBuilder().setName("without-out").addAllOperations(operations).build();
    }

    Graph graphWithRepeatedOps() {
        return graphWithRepeatedOps(storageConfig);
    }

    /*  Graph: 1 --> 2
               1 ----^
     */
    Graph graphWithRepeatedOps(LMST.StorageConfig storageConfig) {
        var repeatedOperation = LWF.Operation.newBuilder()
            .setName("first task prints string 'i-am-hacker' to variable")
            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var operations = List.of(
            repeatedOperation,
            repeatedOperation,
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        return Graph.newBuilder().setName("has-same-ops").setZone("ru-central1-a").addAllOperations(operations).build();
    }

    /*  Graphs:
            single_out:   nothing --> 1
            in_out:       1 --> 2
            fully_cached: 1 --> 2
    */
    List<Graph> sequenceOfGraphs(LMST.StorageConfig storageConfig) {
        var firstOp = LWF.Operation.newBuilder()
            .setName("operation-1")
            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var firstGraph = Graph.newBuilder().setName("single-out").addOperations(firstOp).build();

        var secondOp = LWF.Operation.newBuilder()
            .setName("operation-2")
            .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
            .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/b")
                .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var secondGraph = Graph.newBuilder().setName("in-out").addAllOperations(
            List.of(firstOp, secondOp)).build();

        var sameAsSecond = Graph.newBuilder().setName("fully-cached").addAllOperations(List.of(
            firstOp, secondOp)).build();

        return List.of(firstGraph, secondGraph, sameAsSecond);
    }
}
