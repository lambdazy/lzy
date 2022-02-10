package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

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

    @Override
    public void prepare() throws EnvironmentInstallationException {
        baseEnv.prepare();
    }

    private LzyProcess execInEnv(String command, String[] envp)
        throws LzyExecutionException {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[]{"bash", "-c", command};
        return baseEnv.runProcess(bashCmd, envp);
    }

    @Override
    public LzyProcess runProcess(String... command) throws LzyExecutionException {
        try {
            LOG.info("bash exec in docker env");
            return execInEnv(String.join(" ", command), null);
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) throws LzyExecutionException {
        try {
            LOG.info("bash exec in docker env");
            return execInEnv(String.join(" ", command), envp);
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }
}
