package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class DataFlowGraphTest {

    @Ignore
    @Test
    public void findCycle() {
        var a = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_1_1").setPath("a").build();
        var b = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_1_2").setPath("b").build();
        var c = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_2").setPath("c").build();
        var d = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_3").setPath("d").build();
        var e = LWF.Operation.SlotDescription.newBuilder().setStorageUri("slot_uri_4").setPath("e").build();

        var operations = List.of(
            LWF.Operation.newBuilder().setName("first operation").addAllInputSlots(List.of(e))
                .addAllOutputSlots(List.of(a, b)),
            LWF.Operation.newBuilder().setName("second operation").addAllInputSlots(List.of(a))
                .addAllOutputSlots(List.of(c)),
            LWF.Operation.newBuilder().setName("third operation").addAllInputSlots(List.of(b))
                .addAllOutputSlots(List.of(d)),
            LWF.Operation.newBuilder().setName("fourth operation").addAllInputSlots(List.of(c, d))
                .addAllOutputSlots(List.of(e))
        );

        var dataflowGraph = new DataFlowGraph(operations.stream().map(LWF.Operation.Builder::build).toList());

        Assert.assertTrue(dataflowGraph.hasCycle());
    }
}
