package ai.lzy.worker.env;

import ai.lzy.worker.StreamQueue;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import jakarta.annotation.Nullable;
import org.apache.commons.io.output.NullOutputStream;
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
import java.util.stream.Collectors;

public class DockerEnvironment implements BaseEnvironment {

    private static final Logger LOG = LogManager.getLogger(DockerEnvironment.class);
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();

    public final String sourceImage;
    public final String containerId;

    private DockerEnvironment(String sourceImage, String containerId) {
        this.sourceImage = sourceImage;
        this.containerId = containerId;
    }

    public static DockerEnvironment create(BaseEnvConfig config) {
        final String sourceImage = prepareImage(config);

        LOG.info("Creating container from image={} ...", sourceImage);
        LOG.info("Mount options:\n\t{}", config.mounts().stream()
            .map(it -> it.source() + " -> " + it.target() + (it.isRshared() ? " (R_SHARED)" : ""))
            .collect(Collectors.joining("\n\t")));

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
        hostConfig.withDeviceRequests(List.of(new DeviceRequest()
            .withDriver("nvidia")
            .withCapabilities(List.of(List.of("gpu")))
        ));

        final CreateContainerCmd createContainerCmd = DOCKER.createContainerCmd(sourceImage)
            .withHostConfig(hostConfig)
            .withAttachStdout(true)
            .withAttachStderr(true);

        final var container = createContainerCmd
            .withTty(true)
            .exec();
        final String containerId = container.getId();
        LOG.info("Creating container from image={} done, id={}", sourceImage, containerId);

        LOG.info("Starting env container with id {} ...", containerId);
        DOCKER.startContainerCmd(container.getId()).exec();
        LOG.info("Starting env container with id {} done", containerId);

        return new DockerEnvironment(sourceImage, containerId);
    }

    @Nullable
    public static DockerEnvironment fromExistedContainer(String sourceImage, String containerId) {
        final var imageInspection = DOCKER.inspectImageCmd(sourceImage).exec();
        LOG.info("TMP image: {}", imageInspection.toString());
        final var containerInspection = DOCKER.inspectContainerCmd(containerId).withSize(true).exec();
        LOG.info("TMP container: {}", containerInspection.toString());

        return null; // TODO new DockerEnvironment(sourceImage, containerId);
    }

    @Override
    public void install(StreamQueue out, StreamQueue err) throws EnvironmentInstallationException {
        // TODO(artolord) add stdout/stderr to std streams
    }

    @Override
    public LzyProcess runProcess(String... command) {
        return runProcess(command, null);
    }

    @Override
    public LzyProcess runProcess(String[] command, String[] envp) {
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
        final ExecCreateCmd execCmd = DOCKER.execCreateCmd(containerId)
            .withCmd(command)
            .withAttachStdout(true)
            .withAttachStderr(true);

        if (envp != null && envp.length > 0) {
            execCmd.withEnv(List.of(envp));
        }
        final ExecCreateCmdResponse exec = execCmd.exec();
        LOG.info("Executing cmd {}", String.join(" ", command));

        var feature = new CompletableFuture<>();

        var startCmd = DOCKER.execStartCmd(exec.getId())
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
                        case STDOUT:
                            try {
                                stdout.write(item.getPayload());
                                stdout.flush();
                            } catch (IOException e) {
                                LOG.error("Error while write into stdout log", e);
                            }
                            break;
                        case STDERR:
                            try {
                                stderr.write(item.getPayload());
                                stderr.flush();
                            } catch (IOException e) {
                                LOG.error("Error while write into stderr log", e);
                            }
                            break;
                        default:
                            LOG.info("Got frame "
                                + new String(item.getPayload(), StandardCharsets.UTF_8)
                                + " from unknown stream type "
                                + item.getStreamType());
                    }
                }
            });

        return new LzyProcess() {
            @Override
            public OutputStream in() {
                return new NullOutputStream();
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
                    return Math.toIntExact(DOCKER.inspectExecCmd(exec.getId()).exec().getExitCodeLong());
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
                DOCKER.killContainerCmd(containerId) // TODO(d-kruchinin): execId?
                    .withSignal(String.valueOf(sigValue))
                    .exec();
            }
        };
    }

    @Override
    public void close() throws Exception {
        DOCKER.killContainerCmd(containerId).exec();
    }


    @Override
    public String baseEnvId() {
        return containerId;
    }

    public String getSourceImage() {
        return sourceImage;
    }

    public String getContainerId() {
        return containerId;
    }

    private static String prepareImage(BaseEnvConfig config) {
        LOG.info("Pulling image {} ...", config.image());
        final var pullingImage = DOCKER
            .pullImageCmd(config.image())
            .exec(new PullImageResultCallback());
        try {
            pullingImage.awaitCompletion();
        } catch (InterruptedException e) {
            LOG.error("Pulling image {} was interrupted", config.image());
            throw new RuntimeException(e);
        }
        LOG.info("Pulling image {} done", config.image());
        return config.image();
    }
}
