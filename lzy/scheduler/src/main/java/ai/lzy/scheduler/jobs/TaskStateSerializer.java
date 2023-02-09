package ai.lzy.scheduler.jobs;

import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.providers.JsonJobSerializer;
import jakarta.inject.Singleton;


@Singleton
public class TaskStateSerializer extends JsonJobSerializer<TaskState> {
    protected TaskStateSerializer() {
        super(TaskState.class);
    }
}
