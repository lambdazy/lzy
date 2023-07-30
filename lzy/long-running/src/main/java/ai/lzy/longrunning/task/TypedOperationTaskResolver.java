package ai.lzy.longrunning.task;

public interface TypedOperationTaskResolver extends OperationTaskResolver {
    String type();
}
