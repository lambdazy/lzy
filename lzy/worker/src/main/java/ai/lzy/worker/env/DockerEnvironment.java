package ai.lzy.worker.env;

import ai.lzy.v1.common.LME;
import ai.lzy.logs.StreamQueue;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

public class DockerEnvironment extends BaseEnvironment {

    private static final Logger LOG = LogManager.getLogger(DockerEnvironment.class);

    @Nullable public String containerId = null;

    private final BaseEnvConfig config;
    private final DockerClient client;

    public DockerEnvironment(BaseEnvConfig config, @Nullable LME.DockerCredentials credentials) {
        super();
        this.config = config;
        this.client = generateClient(credentials);
    }

    @Override
    public void install(StreamQueue.LogHandle handle) throws EnvironmentInstallationException {
        if (containerId != null) {
            handle.logOut("Using already running container from cache");
            return;
        }

        try {
            prepareImage(config, handle);
        } catch (Exception e) {
            handle.logErr("Error while pulling image: {}", e);
            throw new RuntimeException(e);
        }

        var sourceImage = config.image();

        handle.logOut("Creating container from image={} ... , config = {}", sourceImage, config);

        final List<Mount> dockerMounts = new ArrayList<>();
        config.mounts().forEach(m -> {
            var mount = new Mount().withType(MountType.BIND).withSource(m.source()).withTarget(m.target());
            if (m.isRshared()) {
                mount.withBindOptions(new BindOptions().withPropagation(BindPropagation.R_SHARED));
            }
            dockerMounts.add(mount);
        });

        final HostConfig hostConfig = new HostConfig();
        hostConfig.withMounts(dockerMounts);

        // --gpus all
        if (config.needGpu()) {
            hostConfig.withDeviceRequests(List.of(new DeviceRequest()
                .withDriver("nvidia")
                .withCapabilities(List.of(List.of("gpu")))
            ))
        .withIpcMode("host")
                    .withUlimits(List.of(
                        new Ulimit("memlock", (long) -1, 0),
                        new Ulimit("stack", (long) 67108864, 0)
                    ));
            }

        final var container = client.createContainerCmd(sourceImage)
            .withHostConfig(hostConfig)
            .withAttachStdout(true)
            .withAttachStderr(true)
            .withTty(true)
            .withEnv(config.envVars())
            .exec();

        final String containerId = container.getId();
        handle.logOut("Creating container from image={} done, id={}", sourceImage, containerId);

        handle.logOut("Starting env container with id {} ...", containerId);
        client.startContainerCmd(container.getId()).exec();
        handle.logOut("Starting env container with id {} done", containerId);

        this.containerId = containerId;
    }

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
        assert containerId != null;

        final int bufferSize = 4096;
        final PipedInputStream stdoutPipe = new PipedInputStream(bufferSize);
        final PipedInputStream stderrPipe = new PipedInputStream(bufferSize);
        final PipedOutputStream stdout;
        final PipedOutputStream stderr;
        try {
            stdout = new PipedOutputStream(stdoutPipe);
            stderr = new PipedOutputStream(stderrPipe);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Creating cmd {}", String.join(" ", command));
        final ExecCreateCmd execCmd = client.execCreateCmd(containerId)
            .withCmd(command)
            .withAttachStdout(true)
            .withAttachStderr(true);

        if (envp != null && envp.length > 0) {
            execCmd.withEnv(List.of(envp));
        }
        final ExecCreateCmdResponse exec = execCmd.exec();
        LOG.info("Executing cmd {}", String.join(" ", command));

        var feature = new CompletableFuture<>();

        var startCmd = client.execStartCmd(exec.getId())
            .exec(new ResultCallbackTemplate<>() {
                @Override
                public void onComplete() {
                    LOG.info("Closing stdout, stderr of cmd {}", String.join(" ", command));
                    try {
                        stdout.close();
                        stderr.close();
                    } catch (IOException e) {
                        LOG.error("Cannot close stderr/stdout slots", e);
                    } catch (Exception e) {
                        LOG.error("Error while completing docker env process: ", e);
                    } finally {
                        feature.complete(null);
                    }
                }

                @Override
                public void onNext(Frame item) {
                    switch (item.getStreamType()) {
                        case STDOUT -> {
                            try {
                                stdout.write(item.getPayload());
                                stdout.flush();
                            } catch (IOException e) {
                                LOG.error("Error while write into stdout log", e);
                            }
                        }
                        case STDERR -> {
                            try {
                                stderr.write(item.getPayload());
                                stderr.flush();
                            } catch (IOException e) {
                                LOG.error("Error while write into stderr log", e);
                            }
                        }
                        default -> LOG.info("Got frame "
                            + new String(item.getPayload(), StandardCharsets.UTF_8)
                            + " from unknown stream type "
                            + item.getStreamType());
                    }
                }
            });

        return new LzyProcess() {
            @Override
            public OutputStream in() {
                return OutputStream.nullOutputStream();
            }

            @Override
            public InputStream out() {
                return stdoutPipe;
            }

            @Override
            public InputStream err() {
                return stderrPipe;
            }

            @Override
            public int waitFor() throws InterruptedException {
                try {
                    feature.get();
                    return Math.toIntExact(client.inspectExecCmd(exec.getId()).exec().getExitCodeLong());
                } catch (InterruptedException e) {
                    try {
                        startCmd.close();
                    } catch (IOException ex) {
                        LOG.error("Error while closing cmd: ", ex);
                    }
                    throw e;
                } catch (ExecutionException e) {
                    // ignored
                    return 1;
                }
            }

            @Override
            public void signal(int sigValue) {
                if (containerId != null) {
                    client.killContainerCmd(containerId)
                        .withSignal(String.valueOf(sigValue))
                        .exec();
                }
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (containerId != null) {
            client.killContainerCmd(containerId).exec();
        }
    }

    public BaseEnvConfig config() {
        return config;
    }

    private void prepareImage(BaseEnvConfig config, StreamQueue.LogHandle handle) {
        handle.logOut("Pulling image {} ...", config.image());
        final var pullingImage = client
            .pullImageCmd(config.image())
            .exec(new PullImageResultCallback());
        try {
            pullingImage.awaitCompletion();
        } catch (InterruptedException e) {
            handle.logErr("Pulling image {} was interrupted", config.image());
            throw new RuntimeException(e);
        }
        handle.logOut("Pulling image {} done", config.image());
    }

    public static DockerClient generateClient(@Nullable LME.DockerCredentials credentials) {
        if (credentials != null) {
            return DockerClientBuilder.getInstance(new DefaultDockerClientConfig.Builder()
                .withRegistryUrl(credentials.getRegistryName())
                .withRegistryUsername(credentials.getUsername())
                .withRegistryPassword(credentials.getPassword())
                .build()
            ).build();
        } else {
            return DockerClientBuilder.getInstance().build();
        }
    }
}
