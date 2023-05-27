package ai.lzy.longrunning.task;

public interface TaskResolver {
    TaskAwareAction resolve(Task task);
}
