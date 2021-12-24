package ru.yandex.cloud.ml.platform.lzy.model.logs;

import java.util.Map;

public class UserEvent extends BaseEvent {
    public enum UserEventType{
        TaskCreate,
        TaskStartUp,
        ExecutionPreparing,
        ExecutionStart,
        ExecutionProgress,
        ExecutionComplete,
        TaskStop,
    }
    private final UserEventType type;

    public UserEventType getType() {
        return type;
    }

    public UserEvent(String description, Map<String, String> tags, UserEventType type){
        super(description, tags);
        this.type = type;
    }
}
