package ai.lzy.graph.algo;

import ai.lzy.graph.model.GraphState;
import ai.lzy.graph.model.TaskSlotDescription;
import ai.lzy.graph.model.TaskState;

import java.util.*;

public class Algorithms {
    public static void buildTaskDependents(GraphState graph, List<TaskState> tasks, List<String> channels) {
        final Map<String, List<TaskState>> channelInputs = new HashMap<>();
        final Map<String, List<TaskState>> channelOutputs = new HashMap<>();
        final Map<String, TaskState> taskMap = new HashMap<>();

        for (var task: tasks) {
            if (taskMap.containsKey(task.id())) {
                throw new IllegalStateException(
                    "Task with id <" + task.id() + "> was found many times in graph description"
                );
            }
            taskMap.put(task.id(), task);
            buildChannelsForTask(task, channelInputs, channelOutputs);
        }

        for (String channelId: channels) {
            for (TaskState input: channelInputs.get(channelId)) {
                for (TaskState output: channelOutputs.get(channelId)) {
                    input.tasksDependedFrom().add(output.id());
                    output.tasksDependedOn().add(input.id());
                }
            }
        }

        if (isCyclic(taskMap)) {
            throw new IllegalStateException(
                "Graph with id <%s> contains cycles".formatted(graph.id())
            );
        }
    }

    private static void buildChannelsForTask(TaskState task,
                                             Map<String, List<TaskState>> channelInputs,
                                             Map<String, List<TaskState>> channelOutputs)
    {
        final List<String> inputs = task.taskSlotDescription().slots().stream()
            .filter(s -> s.direction() == TaskSlotDescription.Slot.Direction.INPUT)
            .map(TaskSlotDescription.Slot::name)
            .toList();

        final List<String> outputs = task.taskSlotDescription().slots().stream()
            .filter(s -> s.direction() == TaskSlotDescription.Slot.Direction.OUTPUT)
            .map(TaskSlotDescription.Slot::name)
            .toList();

        final Set<String> intersection = new HashSet<>(inputs);
        intersection.retainAll(outputs);

        if (intersection.size() > 0) {
            throw new IllegalStateException(
                "Slots %s of task <%s> are inputs and outputs in the same time"
                    .formatted(Arrays.toString(intersection.toArray()), task.id())
            );
        }

        task.taskSlotDescription().slotsToChannelsAssignments().entrySet().stream()
            .filter(e -> outputs.contains(e.getKey()))
            .map(e -> channelInputs.computeIfAbsent(e.getValue(), k -> new ArrayList<>()))
            .forEach(l -> l.add(task));
        task.taskSlotDescription().slotsToChannelsAssignments().entrySet().stream()
            .filter(e -> inputs.contains(e.getKey()))
            .map(e -> channelOutputs.computeIfAbsent(e.getValue(), k -> new ArrayList<>()))
            .forEach(l -> l.add(task));
    }

    private static boolean isCyclic(Map<String, TaskState> taskMap) {
        Set<TaskState> visited = new HashSet<>();
        Set<TaskState> stack = new HashSet<>();

        for (TaskState vertex : taskMap.values()) {
            if (isCyclicUtil(taskMap, vertex, visited, stack)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isCyclicUtil(Map<String, TaskState> taskMap, TaskState task,
                                        Set<TaskState> visited, Set<TaskState> stack)
    {
        if (!stack.add(task)) {
            return true;
        }

        if (!visited.add(task)) {
            stack.remove(task);
            return false;
        }

        for (String childId: task.tasksDependedFrom()) {
            if (isCyclicUtil(taskMap, taskMap.get(childId), visited, stack)) {
                return true;
            }
        }

        stack.remove(task);
        return false;
    }
}
