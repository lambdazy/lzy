package ai.lzy.jobsutils.providers;

import ai.lzy.jobsutils.JobService;

import javax.annotation.Nullable;

public interface JobProvider {
    void execute(@Nullable String serializedInput);

    String serialize(Object input) throws SerializationException;

    class SerializationException extends Exception {
        SerializationException(Throwable e) {
            super(e);
        }
    }
}
