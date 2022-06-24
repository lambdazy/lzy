package ai.lzy.model.logs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.Map;

public class BaseEvent {
    public static final ObjectWriter DEFAULT_OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();

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
        try {
            return DEFAULT_OBJECT_WRITER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{\"error\":\"Cannot convert event to json\"}";
        }
    }
}
