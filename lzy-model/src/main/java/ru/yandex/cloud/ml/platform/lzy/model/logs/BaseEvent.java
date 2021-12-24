package ru.yandex.cloud.ml.platform.lzy.model.logs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.Map;

public class BaseEvent {
    protected final String description;
    protected final Map<String, String> tags;

    public BaseEvent(String description, Map<String, String> tags) {
        this.description = description;
        this.tags = tags;
    }

    public String getDescription() {
        return description;
    }

    public Map<String, String> getTags() {
        return tags;
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
