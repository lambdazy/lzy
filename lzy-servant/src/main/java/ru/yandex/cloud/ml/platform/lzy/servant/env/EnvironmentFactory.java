package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;

public class EnvironmentFactory {

    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);

    public static Environment create(Env env) throws EnvironmentInstallationException {
        final String resourcesPathStr = "/tmp/resources/";

        final BaseEnvironment baseEnv;
        if (env.baseEnv() != null) {
            LOG.info("No baseEnv provided, using ProcessEnvironment");
            baseEnv = new ProcessEnvironment();
        } else {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");
            BaseEnvConfig config = BaseEnvConfig.newBuilder()
                .image(env.baseEnv().name())
                .addMount(resourcesPathStr, resourcesPathStr)
                .build();
            baseEnv = new DockerEnvironment(config);
        }

        if (env.auxEnv() instanceof PythonEnv) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) env.auxEnv(), baseEnv);
        } else {
            LOG.info("No auxEnv provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv);
        }
    }
}
