package ai.lzy.worker;

import ai.lzy.env.Environment;
import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.aux.AuxEnvironment;
import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.env.aux.PlainPythonEnvironment;
import ai.lzy.env.aux.SimpleBashEnvironment;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.base.DockerEnvDescription;
import ai.lzy.env.base.DockerEnvDescription.ContainerRegistryCredentials;
import ai.lzy.env.base.DockerEnvironment;
import ai.lzy.env.base.ProcessEnvironment;
import ai.lzy.env.logs.LogHandle;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LME.LocalModule;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import jakarta.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Singleton
public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static final String RESOURCES_PATH = "/tmp/resources/";
    private static final String LOCAL_MODULES_PATH = "/tmp/local_modules/";
    private static final AtomicBoolean INSTALL_ENV = new AtomicBoolean(true);


    private final HashMap<String, DockerEnvironment> createdContainers = new HashMap<>();
    private final ProcessEnvironment localProcessEnv = new ProcessEnvironment();
    private static Supplier<AuxEnvironment> envForTests = null;

    private final boolean hasGpu;

    public EnvironmentFactory(ServiceConfig config) {
        this.hasGpu = config.getGpuCount() > 0;
    }

    public AuxEnvironment create(String fsRoot, LME.EnvSpec env, LogHandle logHandle, String lzyMount)
        throws EnvironmentInstallationException
    {
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

            var configBuilder = DockerEnvDescription.newBuilder()
                .withGpu(hasGpu)
                .withImage(image)
                .addMount(RESOURCES_PATH, RESOURCES_PATH)
                .addMount(LOCAL_MODULES_PATH, LOCAL_MODULES_PATH)
                .addRsharedMount(fsRoot, fsRoot)
                .withEnvVars(env.getEnvMap())
                .withEnvVars(Map.of(
                    "LZY_INNER_CONTAINER", "true",
                    "LZY_MOUNT", lzyMount
                ));

            if (credentials != null) {
                configBuilder.withCredentials(new ContainerRegistryCredentials(
                    credentials.getRegistryName(), credentials.getUsername(), credentials.getPassword()));
            }

            var config = configBuilder.build();

            var cachedEnv = createdContainers.get(image);

            if (cachedEnv != null) {
                if (env.getDockerPullPolicy() == LME.DockerPullPolicy.ALWAYS) {
                    try {
                        cachedEnv.close();
                    } catch (Exception e) {
                        LOG.error("Cannot kill docker container {}", cachedEnv.containerId, e);
                    }
                    baseEnv = new DockerEnvironment(config);
                    createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
                } else {
                    baseEnv = cachedEnv;
                }

            } else {
                baseEnv = new DockerEnvironment(config);
                createdContainers.put(config.image(), (DockerEnvironment) baseEnv);
            }
        } else {
            baseEnv = localProcessEnv.withEnv(env.getEnvMap())
                .withEnv(Map.of("LZY_MOUNT", lzyMount));
        }

        if (INSTALL_ENV.get()) {
            baseEnv.install(logHandle);
        }

        final AuxEnvironment auxEnv;

        if (env.hasPyenv()) {
            final Environment.LzyProcess proc;

            if (INSTALL_ENV.get()) {
                proc = baseEnv.runProcess("conda", "--version");
            } else {
                proc = completedCondaProcess();  // Do not get actual conda version here, mock it for test
            }
            final int res;

            try {
                res = proc.waitFor();
            } catch (InterruptedException e) {
                LOG.error("Installation of environment interrupted", e);
                throw new EnvironmentInstallationException("Installation of environment interrupted");
            }

            if (res != 0) {
                String err;
                try {
                    err = IOUtils.toString(proc.err(), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    LOG.error("Cannot get error of getting conda version: ", e);
                    err = "";
                }

                var sb = new StringBuilder();
                sb.append("*WARNING* Using plain python environment instead of conda.");
                sb.append(" Your packages will not be installed: \n");
                for (var line : env.getPyenv().getYaml().split("\n")) {
                    sb.append("  > ").append(line).append("\n");
                }
                logHandle.logErr(sb.toString());

                LOG.error("Cannot find conda in provided env, rc={}, env={}: {}", res, env, err);
                auxEnv = new PlainPythonEnvironment(baseEnv, env.getPyenv().getLocalModulesList()
                    .stream()
                    .collect(Collectors.toMap(LocalModule::getName, LocalModule::getUri)), LOCAL_MODULES_PATH);
            } else {
                final String out;

                try {
                    out = IOUtils.toString(proc.out(), StandardCharsets.UTF_8);
                    LOG.info("Using conda with version \"{}\"", out);
                } catch (IOException e) {
                    LOG.error("Cannot find conda version", e);
                }

                auxEnv = new CondaEnvironment(baseEnv, env.getPyenv().getYaml(),
                    env.getPyenv().getLocalModulesList()
                        .stream()
                        .collect(Collectors.toMap(LocalModule::getName, LocalModule::getUri)),
                    RESOURCES_PATH, LOCAL_MODULES_PATH);
            }

        } else if (env.hasProcessEnv()) {
            auxEnv = new SimpleBashEnvironment(baseEnv, Map.of(), Path.of(RESOURCES_PATH));
        } else {
            LOG.error("Error while creating env: undefined env");
            throw Status.UNIMPLEMENTED.withDescription("Provided unsupported env")
                .asRuntimeException();
        }

        if (INSTALL_ENV.get()) {
            auxEnv.install(logHandle);
        }

        return auxEnv;
    }

    private static Environment.LzyProcess completedCondaProcess() {
        return new Environment.LzyProcess() {
            @Override
            public OutputStream in() {
                return null;
            }

            @Override
            public InputStream out() {
                return new ByteArrayInputStream("1.0.0".getBytes());
            }

            @Override
            public InputStream err() {
                return null;
            }

            @Override
            public int waitFor() {
                return 0;
            }

            @Override
            public void signal(int sigValue) {}
        };
    }

    @VisibleForTesting
    public static void envForTests(Supplier<AuxEnvironment> env) {
        envForTests = env;
    }

    @VisibleForTesting
    public static void installEnv(boolean b) {
        INSTALL_ENV.set(b);
    }

}
