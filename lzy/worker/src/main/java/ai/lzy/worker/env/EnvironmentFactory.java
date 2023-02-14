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

    private final String defaultImage;
    private final boolean hasGpu;

    public EnvironmentFactory(String defaultImage, int gpuCount) {
        this.defaultImage = defaultImage;
        this.hasGpu = gpuCount > 0;
    }

    public AuxEnvironment create(String fsRoot, LME.EnvSpec env) {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        if (env.hasAuxEnv() || env.hasBaseEnv()) {  // To support deprecated api
            return getDeprecatedEnvironment(fsRoot, env);
        }
        final BaseEnvironment baseEnv;

        if (!Strings.isBlank(env.getDockerImage())) {
            var image = env.getDockerImage();
            LOG.info("Creating env with docker image {}", image);

            var config = BaseEnvConfig.newBuilder()
                .withGpu(hasGpu)
                .withImage(image)
                .addMount(RESOURCES_PATH, RESOURCES_PATH)
                .addMount(LOCAL_MODULES_PATH, LOCAL_MODULES_PATH)
                .addRsharedMount(fsRoot, fsRoot)
                .setEnvs(env.getEnvVariablesMap())
                .build();

            if (createdContainers.containsKey(image)) {
                baseEnv = createdContainers.get(image);
            } else {
                baseEnv = new DockerEnvironment(config);
                createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
            }
        } else {
            baseEnv = localProcessEnv.withEnv(env.getEnvVariablesMap());
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

    private AuxEnvironment getDeprecatedEnvironment(String fsRoot, LME.EnvSpec env) {
        BaseEnvironment baseEnv = null;

        if (env.hasBaseEnv()) {
            LOG.info("Docker baseEnv provided, using DockerEnvironment");

            String image = env.getBaseEnv().getName();
            if (Strings.isBlank(image) || image.equals("default")) {
                image = defaultImage;
            }

            var config = BaseEnvConfig.newBuilder()
                .withGpu(hasGpu)
                .withImage(image)
                .addMount(RESOURCES_PATH, RESOURCES_PATH)
                .addMount(LOCAL_MODULES_PATH, LOCAL_MODULES_PATH)
                .addRsharedMount(fsRoot, fsRoot)
                .build();

            if (createdContainers.containsKey(config.image())) {
                baseEnv = createdContainers.get(config.image());
            } else {
                baseEnv = new DockerEnvironment(config);
                createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
            }

        } else {
            LOG.info("No baseEnv provided, using ProcessEnvironment");
            baseEnv = localProcessEnv;
        }

        if (env.hasAuxEnv() && env.getAuxEnv().hasPyenv()) {
            LOG.info("Conda auxEnv provided, using CondaEnvironment");
            return new CondaEnvironment(env.getAuxEnv().getPyenv(), baseEnv, RESOURCES_PATH, LOCAL_MODULES_PATH);
        } else {
            LOG.info("No auxEnv provided, using SimpleBashEnvironment");
            return new SimpleBashEnvironment(baseEnv, env.getEnvVariablesMap());
        }
    }

    @VisibleForTesting
    public static void envForTests(Supplier<AuxEnvironment> env) {
        envForTests = env;
    }

}
