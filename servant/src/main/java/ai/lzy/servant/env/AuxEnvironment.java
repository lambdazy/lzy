package ai.lzy.servant.env;

public interface AuxEnvironment extends Environment {
    BaseEnvironment base();

    @Override
    default void close() throws Exception {
        base().close();
    }
}
