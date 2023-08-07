package ai.lzy.graph.model;

import ai.lzy.common.SafeCloseable;
import ai.lzy.graph.LGE;
import jakarta.annotation.Nullable;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class GraphState {
    private static final AtomicBoolean ENABLE_LOCK = new AtomicBoolean(true);

    private final ReentrantLock lock = new ReentrantLock();
    private final String id;
    private final String operationId;
    private Status status;
    private final String executionId;
    private final String workflowName;
    private final String userId;
    private final String allocatorSessionId;
    private final Map<Status, Set<String>> tasks;
    @Nullable
    private String errorDescription;
    @Nullable
    private String failedTaskId;
    @Nullable
    private String failedTaskName;

    public static GraphState createNew(String id, String operationId, String executionId, String workflowName,
                                       String userId, String allocatorSessionId, Set<String> tasks)
    {
        var tasksMap = new EnumMap<Status, Set<String>>(Status.class);
        tasksMap.put(Status.WAITING, tasks);
        for (var st : Status.values()) {
            tasksMap.putIfAbsent(st, new HashSet<>());
        }

        return new GraphState(id, operationId, Status.WAITING, executionId, workflowName, userId, allocatorSessionId,
            tasksMap, null, null, null);
    }

    public GraphState(String id, String operationId, Status status, String executionId, String workflowName,
                      String userId, String allocatorSessionId, Map<Status, Set<String>> tasks,
                      @Nullable String errorDescription, @Nullable String failedTaskId, @Nullable String failedTaskName)
    {
        this.id = id;
        this.operationId = operationId;
        this.status = status;
        this.executionId = executionId;
        this.workflowName = workflowName;
        this.userId = userId;
        this.allocatorSessionId = allocatorSessionId;
        this.tasks = tasks;
        for (var st : Status.values()) {
            this.tasks.putIfAbsent(st, new HashSet<>());
        }
        this.errorDescription = errorDescription;
        this.failedTaskId = failedTaskId;
        this.failedTaskName = failedTaskName;
    }

    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED;

        public boolean finished() {
            return this == COMPLETED || this == FAILED;
        }

        public static Status fromTaskStatus(TaskState.Status status) {
            return switch (status) {
                case WAITING -> Status.WAITING;
                case WAITING_ALLOCATION, ALLOCATING, EXECUTING -> Status.EXECUTING;
                case COMPLETED -> Status.COMPLETED;
                case FAILED -> Status.FAILED;
            };
        }
    }

    @Override
    public String toString() {
        assertHasLock();

        var taskDescr = tasks.keySet().stream()
            .map(key -> "%s = {%s}".formatted(key, String.join(", ", tasks.get(key))))
            .collect(Collectors.joining("\n"));

        return """
            GraphState {
                executionId: %s,
                id: %s,
                status: %s,
                errorDescription: %s,
                tasks: %s
            }"""
            .formatted(executionId, id, status, errorDescription, taskDescr);
    }

    public static GraphState fromProto(LGE.ExecuteGraphRequest request, String graphId, String operationId) {
        return createNew(graphId, operationId, request.getExecutionId(), request.getWorkflowName(), request.getUserId(),
            request.getAllocatorSessionId(), new HashSet<>());
    }

    public LGE.ExecuteGraphMetadata toMetaProto(Function<String, LGE.TaskExecutionStatus> taskStatusProvider) {
        assertHasLock();

        var builder = LGE.ExecuteGraphMetadata.newBuilder()
            .setUserId(userId)
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraphId(id);

        switch (status) {
            case WAITING -> builder.setWaiting(
                LGE.GraphState.Waiting.newBuilder()
                    .build());
            case EXECUTING -> builder.setExecuting(
                LGE.GraphState.Executing.newBuilder()
                    .addAllExecutingTasks(
                        tasks.get(Status.EXECUTING).stream()
                            .map(taskStatusProvider)
                            .toList())
                    .build());
            case COMPLETED -> builder.setCompleted(
                LGE.GraphState.Completed.newBuilder()
                    .build());
            case FAILED -> builder.setFailed(
                LGE.GraphState.Failed.newBuilder()
                    .setFailedTaskId(requireNonNull(failedTaskId))
                    .setFailedTaskName(requireNonNull(failedTaskName))
                    .setDescription(requireNonNull(errorDescription))
                    .build());
        }

        return builder.build();
    }

    @Nullable
    public LGE.ExecuteGraphResponse toResponseProto() {
        assertHasLock();

        return switch (status) {
            case COMPLETED ->
                LGE.ExecuteGraphResponse.newBuilder()
                    .setUserId(userId)
                    .setWorkflowName(workflowName)
                    .setExecutionId(executionId)
                    .setGraphId(id)
                    .setCompleted(LGE.GraphState.Completed.newBuilder().build())
                    .build();
            case FAILED ->
                LGE.ExecuteGraphResponse.newBuilder()
                    .setUserId(userId)
                    .setWorkflowName(workflowName)
                    .setExecutionId(executionId)
                    .setGraphId(id)
                    .setFailed(LGE.GraphState.Failed.newBuilder()
                        .setFailedTaskId(requireNonNull(failedTaskId))
                        .setFailedTaskName(requireNonNull(failedTaskName))
                        .setDescription(requireNonNull(errorDescription))
                        .build())
                    .build();
            case WAITING, EXECUTING ->
                null;
        };
    }

    public void initTasks(Set<String> initTasks) {
        assert status == Status.WAITING;
        var prev = tasks.put(Status.WAITING, initTasks);
        assert prev == null || prev.isEmpty();
    }

    public void tryComplete(String completedTaskId) {
        assertHasLock();

        if (status.finished()) {
            return;
        }

        assert tasks.get(Status.FAILED).isEmpty();

        tasks.get(Status.EXECUTING).remove(completedTaskId);
        tasks.get(Status.COMPLETED).add(completedTaskId);

        if (tasks.get(Status.WAITING).isEmpty() || tasks.get(Status.EXECUTING).isEmpty()) {
            status = Status.COMPLETED;
        }
    }

    public void tryFail(String failedTaskId, String failedTaskName, @Nullable String errorDescription) {
        assertHasLock();

        if (status.finished()) {
            return;
        }

        tasks.get(Status.WAITING).clear();
        tasks.get(Status.EXECUTING).clear();
        tasks.get(Status.FAILED).add(failedTaskId);

        this.status = Status.FAILED;
        this.failedTaskId = failedTaskId;
        this.failedTaskName = failedTaskName;
        this.errorDescription = errorDescription;
    }

    public void tryExecute(String runningTaskId) {
        assertHasLock();

        if (status.finished()) {
            return;
        }

        tasks.get(GraphState.Status.WAITING).remove(runningTaskId);
        tasks.get(GraphState.Status.EXECUTING).add(runningTaskId);

        if (status == Status.WAITING) {
            status = Status.EXECUTING;
        }
    }

    public SafeCloseable bind() {
        lock.lock();
        return lock::unlock;
    }

    public String id() {
        return id;
    }

    public String operationId() {
        return operationId;
    }

    public Status status() {
        assertHasLock();
        return status;
    }

    public String executionId() {
        return executionId;
    }

    public String workflowName() {
        return workflowName;
    }

    public String userId() {
        return userId;
    }

    public String allocatorSessionId() {
        return allocatorSessionId;
    }

    @Nullable
    public String errorDescription() {
        assertHasLock();
        return errorDescription;
    }

    @Nullable
    public String failedTaskId() {
        assertHasLock();
        return failedTaskId;
    }

    @Nullable
    public String failedTaskName() {
        assertHasLock();
        return failedTaskName;
    }

    private void assertHasLock() {
        if (!ENABLE_LOCK.get()) {
            return;
        }
        assert lock.isHeldByCurrentThread();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphState that = (GraphState) o;
        return Objects.equals(id, that.id) && Objects.equals(operationId, that.operationId) && status == that.status &&
            Objects.equals(executionId, that.executionId) && Objects.equals(workflowName, that.workflowName) &&
            Objects.equals(userId, that.userId) && Objects.equals(allocatorSessionId, that.allocatorSessionId) &&
            /* Objects.equals(tasks, that.tasks) && */ Objects.equals(errorDescription, that.errorDescription) &&
            Objects.equals(failedTaskId, that.failedTaskId) && Objects.equals(failedTaskName, that.failedTaskName);
    }

    public static void disableLocking() {
        ENABLE_LOCK.set(false);
    }

    public static void enableLocking() {
        ENABLE_LOCK.set(true);
    }
}

