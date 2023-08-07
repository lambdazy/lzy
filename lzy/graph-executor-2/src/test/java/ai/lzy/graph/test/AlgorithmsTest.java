package ai.lzy.graph.test;

import ai.lzy.graph.algo.Algorithms;
import ai.lzy.graph.model.GraphState;
import ai.lzy.graph.model.TaskSlotDescription;
import ai.lzy.graph.model.TaskSlotDescription.Slot;
import ai.lzy.graph.model.TaskState;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static ai.lzy.graph.model.TaskSlotDescription.Slot.Direction;
import static ai.lzy.graph.model.TaskSlotDescription.Slot.Media;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class AlgorithmsTest {
    private static final Slot SLOT_0 = new Slot("slot0", Media.FILE, Direction.OUTPUT, null, null, null, null);
    private static final Slot SLOT_1 = new Slot("slot1", Media.PIPE, Direction.OUTPUT, null, null, null, null);
    private static final Slot SLOT_2 = new Slot("slot2", Media.PIPE, Direction.INPUT, null, null, null, null);
    private static final Slot SLOT_3 = new Slot("slot3", Media.ARG, Direction.OUTPUT, null, null, null, null);
    private static final Slot SLOT_4 = new Slot("slot4", Media.FILE, Direction.INPUT, null, null, null, null);
    private static final Slot SLOT_5 = new Slot("slot5", Media.ARG, Direction.INPUT, null, null, null, null);
    private static final Slot SLOT_6 = new Slot("slot6", Media.FILE, Direction.OUTPUT, null, null, null, null);
    private static final Slot SLOT_7 = new Slot("slot6", Media.FILE, Direction.INPUT, null, null, null, null);

    private final GraphState graph = new GraphState("graph1", "op1", GraphState.Status.WAITING, "exec1", "workflow1",
        "user1", "sid1", new EnumMap<>(GraphState.Status.class), null, null, null);

    @Test
    public void simpleTest() {
        var task0 = buildTask("task0", List.of(SLOT_0, SLOT_1),
            Map.of("slot0", "ch0", "slot1", "ch1"));
        var task1 = buildTask("task1", List.of(SLOT_2, SLOT_3),
            Map.of("slot2", "ch1", "slot3", "ch2"));
        var task2 = buildTask("task2", List.of(SLOT_4, SLOT_5, SLOT_6),
            Map.of("slot4", "ch0", "slot5", "ch2"));
        Algorithms.buildTaskDependents(graph, List.of(task0, task1, task2), List.of("ch0", "ch1", "ch2"));

        Assert.assertTrue(task0.tasksDependedOn().isEmpty());
        MatcherAssert.assertThat(task0.tasksDependedFrom(), containsInAnyOrder("task1", "task2"));
        MatcherAssert.assertThat(task1.tasksDependedOn(), containsInAnyOrder("task0"));
        MatcherAssert.assertThat(task1.tasksDependedFrom(), containsInAnyOrder("task2"));
        MatcherAssert.assertThat(task2.tasksDependedOn(), containsInAnyOrder("task0", "task1"));
        Assert.assertTrue(task2.tasksDependedFrom().isEmpty());
    }

    @Test
    public void duplicatingTasksTest() {
        var task0 = buildTask("task0", List.of(SLOT_0, SLOT_1),
            Map.of("slot0", "ch0", "slot1", "ch1"));
        var task1 = buildTask("task0", List.of(SLOT_2, SLOT_3),
            Map.of("slot2", "ch1", "slot3", "ch2"));
        var task2 = buildTask("task2", List.of(SLOT_4, SLOT_5, SLOT_6),
            Map.of("slot4", "ch0", "slot5", "ch2"));
        Assert.assertThrows(IllegalStateException.class, () ->
            Algorithms.buildTaskDependents(graph, List.of(task0, task1, task2), List.of("ch0", "ch1", "ch2"))
        );
    }

    @Test
    public void inputOutputSlotTest() {
        var task0 = buildTask("task0", List.of(SLOT_0, SLOT_1),
            Map.of("slot0", "ch0", "slot1", "ch1"));
        var task1 = buildTask("task1", List.of(SLOT_2, SLOT_3),
            Map.of("slot2", "ch1", "slot3", "ch2"));
        var task2 = buildTask("task2", List.of(SLOT_4, SLOT_5, SLOT_6, SLOT_7),
            Map.of("slot4", "ch0", "slot5", "ch2"));
        Assert.assertThrows(IllegalStateException.class, () ->
            Algorithms.buildTaskDependents(graph, List.of(task0, task1, task2), List.of("ch0", "ch1", "ch2"))
        );
    }

    @Test
    public void cyclicTest() {
        var task0 = buildTask("task0", List.of(SLOT_4, SLOT_1),
            Map.of("slot4", "ch0", "slot1", "ch1"));
        var task1 = buildTask("task1", List.of(SLOT_2, SLOT_3),
            Map.of("slot2", "ch1", "slot3", "ch2"));
        var task2 = buildTask("task2", List.of(SLOT_0, SLOT_5, SLOT_6),
            Map.of("slot0", "ch0", "slot5", "ch2"));
        Assert.assertThrows(IllegalStateException.class, () ->
            Algorithms.buildTaskDependents(graph, List.of(task0, task1, task2), List.of("ch0", "ch1", "ch2"))
        );
    }

    private TaskState buildTask(String taskId, List<Slot> slots, Map<String, String> slotToChannels) {
        final TaskSlotDescription slotDescription = new TaskSlotDescription(taskId, "descr",
            "pool", "zone", "command", slots, slotToChannels, null);
        return new TaskState(taskId, taskId, "op1", "graph", TaskState.Status.WAITING, "w1",
            "w1", "user1", "sid1", slotDescription, new ArrayList<>(), new ArrayList<>(), null, null);
    }
}
