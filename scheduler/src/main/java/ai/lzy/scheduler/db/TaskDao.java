package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.task.Task;
import java.util.List;
import javax.annotation.Nullable;

public interface TaskDao {
    Task create(String workflowId, TaskDesc taskDesc);

    @Nullable
    Task get(String workflowId, String taskId);

    List<Servant> filter(String workflowId, ServantState.Status... statuses);

    void update(Task state);
}
