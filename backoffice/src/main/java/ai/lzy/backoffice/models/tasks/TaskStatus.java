package ai.lzy.backoffice.models.tasks;

import ai.lzy.v1.common.LME;
import ai.lzy.v1.deprecated.LzyTask;
import io.micronaut.core.annotation.Introspected;
import java.util.List;
import java.util.stream.Collectors;

@Introspected
public class TaskStatus {

    private String taskId;
    private String owner;
    private String servant;
    private String explanation;
    private String status;
    private String fuze;
    private String description;
    private List<String> tags;

    public static TaskStatus fromModel(LzyTask.TaskStatus task) {
        TaskStatus status = new TaskStatus();
        status.status = task.getStatus().name();
        status.explanation = task.getExplanation();
        status.servant = task.getServant();
        status.taskId = task.getTaskId();
        status.owner = task.getOwner();
        status.fuze = task.getZygote().getFuze();
        status.tags = task.getZygote().getProvisioning().getTagsList().stream()
            .map(LME.Provisioning.Tag::getTag).collect(Collectors.toList());
        status.description = task.getZygote().getDescription();
        return status;
    }

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

    public String getFuze() {
        return fuze;
    }

    public void setFuze(String fuze) {
        this.fuze = fuze;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
