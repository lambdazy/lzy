package ai.lzy.worker.env;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.PythonEnv;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Supplier;

public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static Supplier<Environment> envForTests = null;
    private static boolean IS_DOCKER_SUPPORTED = true;

    public static Environment create(Env env) {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        final String resourcesPathStr = "/tmp/resources/";

        final BaseEnvironment baseEnv;
        if (IS_DOCKER_SUPPORTED && env.baseEnv() != null) {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");
            BaseEnvConfig config = BaseEnvConfig.newBuilder()
                    .image(env.baseEnv().name())
                    .addMount(resourcesPathStr, resourcesPathStr)
                    .build();
            baseEnv = new DockerEnvironment(config);
        } else {
            if (env.baseEnv() == null) {
                LOG.info("No baseEnv provided, using ProcessEnvironment");
            } else if (!IS_DOCKER_SUPPORTED) {
                LOG.info("Docker support disabled, using ProcessEnvironment, "
                         + "baseEnv {} ignored", env.baseEnv().name());
            }
            baseEnv = new ProcessEnvironment();
        }

        if (env.auxEnv() instanceof PythonEnv) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment((PythonEnv) env.auxEnv(), baseEnv);
        } else {
            LOG.info("No auxEnv provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv);
        }
    }

    @VisibleForTesting
    public static void envForTests(Supplier<Environment> env) {
        envForTests = env;
    }

    @VisibleForTesting
    public static void disableDockers() {
        IS_DOCKER_SUPPORTED = false;
    }
}
