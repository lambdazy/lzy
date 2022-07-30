package ai.lzy.backoffice.models.tasks;

import io.micronaut.core.annotation.Introspected;
import java.util.List;
import java.util.stream.Collectors;
import ai.lzy.v1.BackOffice;

@Introspected
public class GetTasksResponse {

    List<TaskStatus> tasks;

    public static GetTasksResponse fromModel(BackOffice.GetTasksResponse response) {
        GetTasksResponse resp = new GetTasksResponse();
        resp.tasks = response.getTasks().getTasksList().stream().map(TaskStatus::fromModel)
            .collect(Collectors.toList());
        return resp;
    }

    public List<TaskStatus> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskStatus> tasks) {
        this.tasks = tasks;
    }
}
