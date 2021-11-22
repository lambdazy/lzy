package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

import java.util.List;
import java.util.stream.Collectors;

@Introspected
public class GetTasksResponse {
    List<TaskStatus> tasks;

    public List<TaskStatus> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskStatus> tasks) {
        this.tasks = tasks;
    }

    public static GetTasksResponse fromModel(BackOffice.GetTasksResponse response){
        GetTasksResponse resp = new GetTasksResponse();
        resp.tasks = response.getTasks().getTasksList().stream().map(TaskStatus::fromModel).collect(Collectors.toList());
        TaskStatus testStatus = new TaskStatus();
        testStatus.setStatus("COMPLETED");
        testStatus.setTaskId("afjaisdfjnfi13143421nsjkdnfo");
        resp.tasks.add(testStatus);
        testStatus = new TaskStatus();
        testStatus.setStatus("OK");
        testStatus.setTaskId("doasjfoiasdjfopsajdfo");
        resp.tasks.add(testStatus);
        return resp;
    }
}
