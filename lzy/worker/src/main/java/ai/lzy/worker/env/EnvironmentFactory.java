package ai.lzy.worker.env;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.PythonEnv;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.function.Supplier;

public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static Supplier<Environment> envForTests = null;

    private final String defaultImage;
    private final boolean isDockerSupported;

    public EnvironmentFactory(String defaultImage) {
        this.defaultImage = defaultImage;
        this.isDockerSupported = true;
    }

    public Environment create(String fsRoot, Env env) {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        final String resourcesPathStr = "/tmp/resources/";
        final String localModulesPathStr = "/tmp/local-modules-" + UUID.randomUUID();

        final BaseEnvironment baseEnv;
        if (isDockerSupported && env.baseEnv() != null) {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");
            String image = env.baseEnv().name();
            BaseEnvConfig config = BaseEnvConfig.newBuilder()
                .image((image == null || image.equals("default")) ? defaultImage : image)
                .addMount(resourcesPathStr, resourcesPathStr)
                .addMount(localModulesPathStr, localModulesPathStr)
                .addRsharedMount(fsRoot, fsRoot)
                .build();
            baseEnv = new DockerEnvironment(config);
        } else {
            if (env.baseEnv() == null) {
                LOG.info("No baseEnv provided, using ProcessEnvironment");
            } else if (!isDockerSupported) {
                LOG.info("Docker support disabled, using ProcessEnvironment, "
                         + "baseEnv {} ignored", env.baseEnv().name());
            }
            baseEnv = new ProcessEnvironment();
        }

        if (env.auxEnv() instanceof PythonEnv) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) env.auxEnv(), baseEnv, resourcesPathStr, localModulesPathStr);
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
