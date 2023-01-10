package ai.lzy.jobsutils.providers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

/**
 * Job provider to serialize/deserialize data with jackson
 * @param <T> Must be JsonSerializable and JsonDeserializable by jackson
 */
public abstract class JobProviderBase<T> implements JobProvider {

    private static final Logger LOG = LogManager.getLogger(WaitForOperation.class);
    final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    final Class<T> type;

    protected JobProviderBase(Class<T> type) {
        this.type = type;
    }

    protected abstract void execute(T arg);

    @Override
    public void execute(@Nullable String serializedInput) {
        final T data;
        try {
            data = mapper.readValue(serializedInput, type);
        } catch (JsonProcessingException e) {
            LOG.error("Error while parsing data: ", e);
            return;
        }
        execute(data);
    }

    @Override
    public String serialize(Object input) throws SerializationException {
        try {
            return mapper.writeValueAsString(input);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}
