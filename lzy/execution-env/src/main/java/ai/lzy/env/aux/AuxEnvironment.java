package ai.lzy.env.aux;

import ai.lzy.env.Environment;
import ai.lzy.env.base.BaseEnvironment;

import java.nio.file.Path;

public interface AuxEnvironment extends Environment {
    BaseEnvironment base();

    /**
     * Returns path to working directory of environment
     */
    Path workingDirectory();

    @Override
    default void close() throws Exception {
        base().close();
    }
}
