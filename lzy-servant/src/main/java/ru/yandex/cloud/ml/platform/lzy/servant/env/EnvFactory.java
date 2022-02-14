package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.BaseEnv;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;

public class EnvFactory {

    private static final Logger LOG = LogManager.getLogger(EnvFactory.class);

    public static Environment create(Env env, BaseEnvConfig config)
        throws EnvironmentInstallationException {
        final String resourcesPathStr = "/tmp/resources/";
        config.addMount(resourcesPathStr, resourcesPathStr);
        final BaseEnvironment baseEnv;
        if (env instanceof BaseEnv) {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");
            baseEnv = new DockerEnvironment(config);
        } else {
            LOG.info("No baseEnv provided, using ProcessEnvironment");
            baseEnv = new ProcessEnvironment();
        }

        if (env instanceof PythonEnv) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) env, baseEnv);
        } else {
            LOG.info("No auxEnv provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv);
        }
    }
}
