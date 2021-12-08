package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.graph.DockerEnv;
import ru.yandex.cloud.ml.platform.lzy.model.graph.PythonEnv;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.servant.env.EnvConfig.MountDescription;

public class EnvFactory {

    private static final Logger LOG = LogManager.getLogger(EnvFactory.class);

    public static Environment create(AtomicZygote zygote, EnvConfig config)
        throws EnvironmentInstallationException {
        final String resourcesPathStr = "/tmp/resources/";
        config.mounts.add(new MountDescription(resourcesPathStr, resourcesPathStr));
        if (zygote == null) {
            return new SimpleBashEnvironment(config);
        }

        if (zygote.env() instanceof PythonEnv) {
            LOG.info("Conda environment is provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) zygote.env(), config);
        } else if (zygote.env() instanceof DockerEnv) {
            LOG.info("Docker environment is provided, using DockerEnvironment");
            return new BaseEnv(config);
        } else {
            LOG.info("No environment provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(config);
        }
    }
}
