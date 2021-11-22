package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

@Introspected
public class TaskStatus {
    private String taskId;
    private String owner;
    private String servant;
    private String explanation;
    private String status;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getServant() {
        return servant;
    }

    public void setServant(String servant) {
        this.servant = servant;
    }

    public String getExplanation() {
        return explanation;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public static TaskStatus fromModel(Tasks.TaskStatus task){
        TaskStatus status = new TaskStatus();
        status.status = task.getStatus().name();
        status.explanation = task.getExplanation();
        status.servant = task.getServant();
        status.taskId = task.getTaskId();
        status.owner = task.getOwner();
        return status;
    }
}
