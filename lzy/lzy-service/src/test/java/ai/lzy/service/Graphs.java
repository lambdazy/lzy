package ai.lzy.service;

import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;

import java.util.List;

public abstract class Graphs {
    private Graphs() {}

    public static String buildSlotUri(String key, LMST.StorageConfig storageConfig) {
        return storageConfig.getUri() + "/" + key;
    }

    public static LWF.Graph emptyGraph() {
        return LWF.Graph.newBuilder().setName("empty-graph").build();
    }

    public static LWF.Graph simpleGraph(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("simple-graph").setZone("ru-central1-a").addAllOperations(operations)
            .build();
    }

    public static LWF.Graph cyclicGraph(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("cyclic-graph").addAllOperations(operationsWithCycleDependency).build();
    }

    public static LWF.Graph nonSuitableZoneGraph(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("pool-non-exists").setZone("ru-central1-a").addOperations(operation)
            .build();
    }

    public static LWF.Graph invalidZoneGraph(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("invalid-pool").addOperations(operation).build();
    }

    public static LWF.Graph unknownSlotUriGraph(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("unknown-slot-uri").addOperations(operation).build();
    }

    public static LWF.Graph withMissingOutputSlot(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("without-out").addAllOperations(operations).build();
    }

    /*  Graph: 1 --> 2
               1 ----^
     */
    public static LWF.Graph graphWithRepeatedOps(LMST.StorageConfig storageConfig) {
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

        return LWF.Graph.newBuilder().setName("has-same-ops").setZone("ru-central1-a").addAllOperations(operations)
            .build();
    }

    /*  Graphs:
            nothing --> 1
            1 --> 2
            1 --> 3
    */
    public static List<LWF.Graph> producerAndConsumersGraphs(LMST.StorageConfig storageConfig) {
        var firstOp = LWF.Operation.newBuilder()
            .setName("operation-1")
            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var firstGraph = LWF.Graph.newBuilder().setName("producer").addOperations(firstOp).build();

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
        var secondGraph = LWF.Graph.newBuilder().setName("consumer-1").addOperations(secondOp).build();

        var thirdOp = LWF.Operation.newBuilder()
            .setName("operation-3")
            .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/c && $LZY_MOUNT/sbin/cat $LZY_MOUNT/b")
            .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/b")
                .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                .build())
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/c")
                .setStorageUri(buildSlotUri("snapshot_c_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var thirdGraph = LWF.Graph.newBuilder().setName("consumer-2").addOperations(thirdOp).build();

        return List.of(firstGraph, secondGraph, thirdGraph);
    }

    /*  Graphs:
            single_out:   nothing --> 1
            in_out:       1 --> 2
            fully_cached: 1 --> 2
    */
    public static List<LWF.Graph> sequenceOfGraphs(LMST.StorageConfig storageConfig) {
        var firstOp = LWF.Operation.newBuilder()
            .setName("operation-1")
            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var firstGraph = LWF.Graph.newBuilder().setName("single-out").addOperations(firstOp).build();

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
        var secondGraph = LWF.Graph.newBuilder().setName("in-out").addAllOperations(
            List.of(firstOp, secondOp)).build();

        var sameAsSecond = LWF.Graph.newBuilder().setName("fully-cached").addAllOperations(List.of(
            firstOp, secondOp)).build();

        return List.of(firstGraph, secondGraph, sameAsSecond);
    }
}
