package ru.yandex.cloud.ml.platform.lzy.servant.env;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.DockerEnv;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvConfig.MountDescription;

public class EnvFactory {

    private static final Logger LOG = LogManager.getLogger(EnvFactory.class);

    public static Environment create(AtomicZygote zygote, EnvConfig config)
        throws EnvironmentInstallationException {
        final String resourcesPathStr = "/tmp/resources/";
        config.mounts.add(new MountDescription(resourcesPathStr, resourcesPathStr));
        final BaseEnvironment baseEnv = (zygote == null || !(zygote.env() instanceof DockerEnv)) ?
            new ProcessEnvironment():
            new DockerEnvironment(config);

        if (zygote == null) {
            return new SimpleBashEnvironment(baseEnv);
        }

        if (zygote.env() instanceof PythonEnv) {
            LOG.info("Conda environment is provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) zygote.env(), baseEnv);
        } else {
            LOG.info("No environment provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv);
        }
    }
}
