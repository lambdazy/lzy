package ru.yandex.cloud.ml.platform.lzy.model.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.Map;

public class UserEvent {
    public enum UserEventType{
        TaskCreate,
        TaskStartUp,
        ExecutionPreparing,
        ExecutionStart,
        ExecutionProgress,
        ExecutionComplete,
        TaskStop,
    }
    String description;
    Map<String, String> tags;
    UserEventType type;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public UserEventType getType() {
        return type;
    }

    public void setType(UserEventType type) {
        this.type = type;
    }

    public UserEvent(String description, Map<String, String> tags, UserEventType type){
        this.description = description;
        this.tags = tags;
        this.type = type;
    }

    public String toJson() {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            return ow.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{\"error\":\"Cannot convert event to json\"}";
        }
    }
}
