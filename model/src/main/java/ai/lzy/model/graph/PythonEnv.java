package ai.lzy.model.graph;

import ai.lzy.model.StorageCredentials;
import javax.annotation.Nullable;
import java.util.List;

public interface PythonEnv extends AuxEnv {
    String name();

    String yaml();

    List<LocalModule> localModules();

    @Nullable
    StorageCredentials credentials();
}
