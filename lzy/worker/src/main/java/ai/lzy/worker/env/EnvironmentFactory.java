package ai.lzy.worker.env;

import ai.lzy.v1.common.LME;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static final String RESOURCES_PATH = "/tmp/resources/";
    private static final String LOCAL_MODULES_PATH = "/tmp/local_modules/";


    private final HashMap<String, DockerEnvironment> createdContainers = new HashMap<>();
    private final ProcessEnvironment localProcessEnv = new ProcessEnvironment();
    private static Supplier<AuxEnvironment> envForTests = null;

    private final boolean hasGpu;

    public EnvironmentFactory(int gpuCount) {
        this.hasGpu = gpuCount > 0;
    }

    public AuxEnvironment create(String fsRoot, LME.EnvSpec env) {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        final BaseEnvironment baseEnv;

        if (!Strings.isBlank(env.getDockerImage())) {
            var image = env.getDockerImage();
            LOG.info("Creating env with docker image {}", image);

            var credentials = env.hasDockerCredentials() ? env.getDockerCredentials() : null;

            var config = BaseEnvConfig.newBuilder()
                .withGpu(hasGpu)
                .withImage(image)
                .addMount(RESOURCES_PATH, RESOURCES_PATH)
                .addMount(LOCAL_MODULES_PATH, LOCAL_MODULES_PATH)
                .addRsharedMount(fsRoot, fsRoot)
                .withEnvVars(env.getEnvMap())
                .withEnvVars(Map.of("LZY_INNER_CONTAINER", "true"))
                .build();

            var cachedEnv = createdContainers.get(image);

            if (cachedEnv != null) {
                if (env.getDockerPullPolicy() == LME.DockerPullPolicy.ALWAYS) {
                    try {
                        cachedEnv.close();
                    } catch (Exception e) {
                        LOG.error("Cannot kill docker container {}", cachedEnv.containerId, e);
                    }
                    baseEnv = new DockerEnvironment(config, credentials);
                    createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
                } else {
                    baseEnv = cachedEnv;
                }

            } else {
                baseEnv = new DockerEnvironment(config, credentials);
                createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
            }
        } else {
            baseEnv = localProcessEnv.withEnv(env.getEnvMap());
        }

        if (env.hasPyenv()) {
            return new CondaEnvironment(env.getPyenv(), baseEnv, RESOURCES_PATH, LOCAL_MODULES_PATH);
        } else if (env.hasProcessEnv()) {
            return new SimpleBashEnvironment(baseEnv, Map.of());
        } else {
            LOG.error("Error while creating env: undefined env");
            throw Status.UNIMPLEMENTED.withDescription("Provided unsupported env")
                .asRuntimeException();
        }
    }

    @VisibleForTesting
    public static void envForTests(Supplier<AuxEnvironment> env) {
        envForTests = env;
    }

}
