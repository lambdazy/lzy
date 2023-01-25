package ai.lzy.scheduler.providers;

public abstract class JobSerializerBase<T> implements JobSerializer {
    protected final Class<T> dataClass;

    protected abstract String serializeArg(T arg) throws SerializationException;
    protected abstract T deserializeArg(String serializedArg) throws SerializationException;


    protected JobSerializerBase(Class<T> dataClass) {
        this.dataClass = dataClass;
    }

    @Override
    public String serialize(Object arg) throws SerializationException {
        if (!dataClass.isInstance(arg)) {
            throw new SerializationException("Cannot cast {} to {}", arg.getClass().getName(), dataClass.getName());
        }

        return serializeArg(dataClass.cast(arg));
    }

    @Override
    public Object deserialize(String serializedArg) throws SerializationException {
        return deserializeArg(serializedArg);
    }
}
