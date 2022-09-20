package ai.lzy.model.graph;

import ai.lzy.model.StorageCredentials;

import java.util.List;
import javax.annotation.Nullable;

public interface PythonEnv extends AuxEnv {
    String name();

    String yaml();

    List<LocalModule> localModules();

    @Nullable
    StorageCredentials credentials();
}
