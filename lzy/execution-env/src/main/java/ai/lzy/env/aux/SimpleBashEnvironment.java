package ai.lzy.env.aux;

import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.logs.LogStream;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleBashEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(SimpleBashEnvironment.class);

    private final BaseEnvironment baseEnv;
    private final List<String> envList;
    private final Path workingDirectory;

    public SimpleBashEnvironment(BaseEnvironment baseEnv, Map<String, String> envList, Path workingDirectory) {
        this.baseEnv = baseEnv;
        this.envList = envList.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList();
        this.workingDirectory = workingDirectory;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    private LzyProcess execInEnv(String command, @Nullable String[] envp, @Nullable String workingDir) {
        LOG.info("Executing command `{}`", command);
        String[] bashCmd = new String[]{"/bin/bash", "-c", command};

        var env = new ArrayList<>(envList);

        if (envp != null) {
            Collections.addAll(env, envp);
        }

        return baseEnv.runProcess(bashCmd, env.toArray(String[]::new),
            workingDir == null ? workingDirectory.toString() : workingDir);
    }

    @Override
    public void install(LogStream outStream, LogStream errStream) {}

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp, @Nullable String workingDir) {
        return execInEnv(String.join(" ", command), envp, workingDir);
    }

    @Override
    public Path workingDirectory() {
        return workingDirectory;
    }
}
