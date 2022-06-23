package ai.lzy.model.logs;

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
        TaskCreate,
        TaskStartUp,
        ExecutionPreparing,
        ExecutionStart,
        ExecutionProgress,
        ExecutionComplete,
        TaskStop,
    }
}
