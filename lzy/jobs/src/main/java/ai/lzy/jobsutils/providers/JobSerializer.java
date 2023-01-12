package ai.lzy.jobsutils.providers;

public interface JobSerializer {
    String serialize(Object arg) throws SerializationException;
    Object deserialize(String serializedArg) throws SerializationException;

    class SerializationException extends Exception {
        SerializationException(Throwable e) {
            super(e);
        }

        SerializationException(String message, Object... args) {
            super(String.format(message, args));
        }
    }
}
