package ai.lzy.logs;

import java.util.Map;

public class UserEvent extends BaseEvent {
    private final UserEventType type;

    public UserEvent(String description, Map<String, String> tags, UserEventType type) {
        super(description, tags);
        this.type = type;
    }

    public UserEventType getType() {
        return type;
    }

    public enum UserEventType {
        // TODO (lindvv): Think about it after turning on scheduler.
        //      Delete unused statuses. Decide which statuses should be on worker, on scheduler, on smth else.
        TaskCreate,
        TaskStartUp,
        ExecutionPreparing,
        ExecutionStart,
        ExecutionProgress,
        ExecutionComplete,
        TaskStop,
    }
}
