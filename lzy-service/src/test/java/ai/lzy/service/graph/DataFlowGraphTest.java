package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DataFlowGraphTest {

    @Test
    public void findCycle() {
        var a = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_1_1").setPath("a").build();
        var b = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_1_2").setPath("b").build();
        var c = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_2").setPath("c").build();
        var d = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_3").setPath("d").build();
        var e = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_4").setPath("e").build();

        var nodes = List.of(
            new DataFlowGraph.Node("first operation", List.of(e), List.of(a, b)),
            new DataFlowGraph.Node("second operation", List.of(a), List.of(c)),
            new DataFlowGraph.Node("third operation", List.of(b), List.of(d)),
            new DataFlowGraph.Node("fourth operation", List.of(c, d), List.of(e))
        );

        var dataflowGraph = new DataFlowGraph(nodes);

        Assert.assertTrue(dataflowGraph.hasCycle());
    }
}
