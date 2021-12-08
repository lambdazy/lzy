package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public class SimpleBashEnvironment extends BaseEnv {
    private static final Logger LOG = LogManager.getLogger(SimpleBashEnvironment.class);

    public SimpleBashEnvironment(EnvConfig config) {
        super(config);
    }

    @Override
    public void prepare() throws EnvironmentInstallationException {}

    private LzyProcess execInEnv(String command, String[] envp)
        throws EnvironmentInstallationException, LzyExecutionException {
        LOG.info("Executing command " + command);
        String[] bashCmd = new String[]{"bash", "-c", command};
        return super.runProcess(bashCmd, envp);
    }

    @Override
    public LzyProcess runProcess(String... command)
        throws EnvironmentInstallationException, LzyExecutionException {

        try {
            LOG.info("bash exec in docker env");
            return execInEnv(String.join(" ", command), null);
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }

    @Override
    public LzyProcess runProcess(String command, String[] envp)
        throws EnvironmentInstallationException, LzyExecutionException {

        try {
            LOG.info("bash exec in docker env");
            return execInEnv(String.join(" ", command), envp);
        } catch (Exception e) {
            throw new LzyExecutionException(e);
        }
    }
}
