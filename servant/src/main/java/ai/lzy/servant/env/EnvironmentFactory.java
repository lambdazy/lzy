package ai.lzy.servant.env;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.PythonEnv;
import ai.lzy.fs.storage.StorageClient;

import java.util.function.Supplier;

public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static Supplier<Environment> envForTests = null;

    public static Environment create(Env env, StorageClient storage) throws EnvironmentInstallationException {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        final String resourcesPathStr = "/tmp/resources/";
        final boolean dockerSupported = Boolean.parseBoolean(
                System.getProperty("servant.dockerSupport.enabled", "true")
        );

        final BaseEnvironment baseEnv;
        if (dockerSupported && env.baseEnv() != null) {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");
            BaseEnvConfig config = BaseEnvConfig.newBuilder()
                    .image(env.baseEnv().name())
                    .addMount(resourcesPathStr, resourcesPathStr)
                    .build();
            baseEnv = new DockerEnvironment(config);
        } else {
            if (env.baseEnv() == null) {
                LOG.info("No baseEnv provided, using ProcessEnvironment");
            } else if (!dockerSupported) {
                LOG.info("Docker support disabled, using ProcessEnvironment");
            }
            baseEnv = new ProcessEnvironment();
        }

        if (env.auxEnv() instanceof PythonEnv) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) env.auxEnv(), baseEnv, storage, resourcesPathStr);
        } else {
            LOG.info("No auxEnv provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv);
        }
    }

    @VisibleForTesting
    public static void envForTests(Supplier<Environment> env) {
        envForTests = env;
    }
}
