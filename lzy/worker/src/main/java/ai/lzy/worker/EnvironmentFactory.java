package ai.lzy.worker;

import ai.lzy.env.Environment;
import ai.lzy.env.EnvironmentInstallationException;
import ai.lzy.env.aux.AuxEnvironment;
import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.env.aux.PlainPythonEnvironment;
import ai.lzy.env.aux.SimpleBashEnvironment;
import ai.lzy.env.base.BaseEnvironment;
import ai.lzy.env.base.DockerEnvDescription;
import ai.lzy.env.base.DockerEnvironment;
import ai.lzy.env.base.ProcessEnvironment;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LME.LocalModule;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Singleton
public class EnvironmentFactory {
    private static final Logger LOG = LogManager.getLogger(EnvironmentFactory.class);
    private static final String RESOURCES_PATH = "/tmp/resources/";
    private static final AtomicBoolean INSTALL_ENV = new AtomicBoolean(true);


    private final HashMap<String, DockerEnvironment> createdContainers = new HashMap<>();
    private final ProcessEnvironment localProcessEnv = new ProcessEnvironment();
    private static Supplier<AuxEnvironment> envForTests = null;

    private final boolean hasGpu;

    public EnvironmentFactory(ServiceConfig config) {
        this.hasGpu = config.getGpuCount() > 0;
    }

    public AuxEnvironment create(String fsRoot, LME.EnvSpec env, String lzyMount, LogStreams logStreams)
        throws EnvironmentInstallationException
    {
        //to mock environment in tests
        if (envForTests != null) {
            LOG.info("EnvironmentFactory: using mocked environment");
            return envForTests.get();
        }

        var resourcesDir = Path.of(RESOURCES_PATH, UUID.randomUUID().toString());

        try {
            Files.createDirectories(resourcesDir);
        } catch (Exception e) {
            LOG.error("Cannot create resources directories: ", e);
            throw new EnvironmentInstallationException(e);
        }

        final BaseEnvironment baseEnv;

        if (!Strings.isBlank(env.getDockerImage())) {
            var image = env.getDockerImage();
            LOG.info("Creating env with docker image {}", image);

            logStreams.stdout.log("Creating env with docker image " + image);

            var credentials = env.hasDockerCredentials() ? env.getDockerCredentials() : null;

            var configBuilder = DockerEnvDescription.newBuilder()
                .withGpu(hasGpu)
                .withImage(image)
                .addMount(RESOURCES_PATH, RESOURCES_PATH)
                .addRsharedMount(fsRoot, fsRoot)
                .withEnvVars(env.getEnvMap())
                .withEnvVars(Map.of(
                    "LZY_INNER_CONTAINER", "true",
                    "LZY_MOUNT", lzyMount
                ))
                .withDockerClientConfig(getDockerConfig(credentials));

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
            baseEnv.install(logStreams.systemInfo, logStreams.systemErr);
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

            var localModules = env.getPyenv().getLocalModulesList().stream()
                .collect(Collectors.toMap(LocalModule::getName, LocalModule::getUri));

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
                logStreams.stderr.log(sb.toString());

                LOG.error("Cannot find conda in provided env, rc={}, env={}: {}", res, env, err);
                auxEnv = new PlainPythonEnvironment(baseEnv, localModules, resourcesDir, resourcesDir);
            } else {
                final String out;

                try {
                    out = IOUtils.toString(proc.out(), StandardCharsets.UTF_8).trim();
                    LOG.info("Using conda with version \"{}\"", out);
                    logStreams.stdout.log("Using conda with version \"%s\"".formatted(out));
                } catch (IOException e) {
                    LOG.error("Cannot find conda version", e);
                }

                auxEnv = new CondaEnvironment(baseEnv, env.getPyenv().getYaml(), localModules, resourcesDir,
                    resourcesDir);
            }
        } else if (env.hasProcessEnv()) {
            auxEnv = new SimpleBashEnvironment(baseEnv, Map.of(), resourcesDir);
        } else {
            LOG.error("Error while creating env: undefined env");
            throw Status.UNIMPLEMENTED.withDescription("Provided unsupported env")
                .asRuntimeException();
        }

        if (INSTALL_ENV.get()) {
            auxEnv.install(logStreams.systemInfo, logStreams.systemErr);
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

    private static DockerClientConfig getDockerConfig(@Nullable LME.DockerCredentials credentials) {
        var builder = DefaultDockerClientConfig.createDefaultConfigBuilder();
        final var file = new File(System.getProperty("user.home"), ".docker");
        if (file.exists()) {
            LOG.debug("docker config: {}", file.getAbsolutePath());
            builder.withDockerConfig(file.getAbsolutePath());
        }
        if (credentials != null) {
            builder.withRegistryUrl(credentials.getRegistryName());
            builder.withRegistryUsername(credentials.getUsername());
            builder.withRegistryPassword(credentials.getPassword());
        }
        return builder.build();
    }
}
