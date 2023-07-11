package ai.lzy.env.aux;

import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.LogHandle;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleBashEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(SimpleBashEnvironment.class);

    private final String taskId;
    private final BaseEnvironment baseEnv;
    private final List<String> envList;

    public SimpleBashEnvironment(String taskId, BaseEnvironment baseEnv, Map<String, String> envList) {
        this.taskId = taskId;
        this.baseEnv = baseEnv;
        this.envList = envList.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList();
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    private LzyProcess execInEnv(String command, @Nullable String[] envp) {
        LOG.info("[tid={}] Executing command `{}`", taskId, command);
        String[] bashCmd = new String[]{"bash", "-c", command};

        var env = new ArrayList<>(envList);

        if (envp != null) {
            Collections.addAll(env, envp);
        }

        return baseEnv.runProcess(bashCmd, env.toArray(String[]::new));
    }

    @Override
    public void install(LogHandle logHandle) {}

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp) {
        return execInEnv(String.join(" ", command), envp);
    }
}
