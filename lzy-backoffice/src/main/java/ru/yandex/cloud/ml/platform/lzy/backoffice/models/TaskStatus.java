package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.List;
import java.util.stream.Collectors;

@Introspected
public class TaskStatus {
    private String taskId;
    private String owner;
    private String servant;
    private String explanation;
    private String status;
    private String fuse;
    private List<String> tags;

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

    public String getFuse() {
        return fuse;
    }

    public void setFuse(String fuse) {
        this.fuse = fuse;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public static TaskStatus fromModel(Tasks.TaskStatus task){
        TaskStatus status = new TaskStatus();
        status.status = task.getStatus().name();
        status.explanation = task.getExplanation();
        status.servant = task.getServant();
        status.taskId = task.getTaskId();
        status.owner = task.getOwner();
        status.fuse = task.getFuse();
        status.tags = task.getProvisioning().getTagsList().stream().map(Operations.Provisioning.Tag::getTag).collect(Collectors.toList());
        return status;
    }
}
