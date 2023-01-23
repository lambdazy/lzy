package ai.lzy.scheduler.providers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonJobSerializer<T> extends JobSerializerBase<T> {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Override
    protected String serializeArg(T arg) throws SerializationException {
        try {
            return mapper.writeValueAsString(arg);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    protected T deserializeArg(String serializedArg) throws SerializationException {
        try {
            return mapper.readValue(serializedArg, dataClass);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    protected JsonJobSerializer(Class<T> dataClass) {
        super(dataClass);
    }
}
