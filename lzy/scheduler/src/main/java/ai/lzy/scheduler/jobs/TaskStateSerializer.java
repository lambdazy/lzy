package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.providers.JsonJobSerializer;
import ai.lzy.scheduler.models.TaskState;
import jakarta.inject.Singleton;


@Singleton
public class TaskStateSerializer extends JsonJobSerializer<TaskState> {
    protected TaskStateSerializer() {
        super(TaskState.class);
    }
}
