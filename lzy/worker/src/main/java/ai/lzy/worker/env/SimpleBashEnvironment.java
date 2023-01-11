package ai.lzy.worker.env;

import ai.lzy.worker.StreamQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleBashEnvironment implements AuxEnvironment {
    private static final Logger LOG = LogManager.getLogger(SimpleBashEnvironment.class);
    private final BaseEnvironment baseEnv;

    public SimpleBashEnvironment(BaseEnvironment baseEnv) {
        this.baseEnv = baseEnv;
    }

    @Override
    public BaseEnvironment base() {
        return baseEnv;
    }

    private LzyProcess execInEnv(String command, String[] envp) {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[]{"bash", "-c", command};
        return baseEnv.runProcess(bashCmd, envp);
    }

    @Override
    public void install(StreamQueue out, StreamQueue err) {}

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        return execInEnv(String.join(" ", command), envp);
    }
}
