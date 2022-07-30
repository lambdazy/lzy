package ai.lzy.model.graph;

import java.util.List;

public interface PythonEnv extends AuxEnv {
    String name();

    String yaml();

    List<LocalModule> localModules();
}
