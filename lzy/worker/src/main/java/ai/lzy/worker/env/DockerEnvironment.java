package ai.lzy.worker.env;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.core.DockerClientBuilder;
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
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class DockerEnvironment implements BaseEnvironment {

    private static final Logger LOG = LogManager.getLogger(DockerEnvironment.class);
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();

    public final CreateContainerResponse container;
    public final String sourceImage;

    public DockerEnvironment(BaseEnvConfig config) {
        sourceImage = prepareImage(config);

        LOG.info("Creating container from image={} ...", sourceImage);
        LOG.info("Mount options:\n\t{}", config.mounts().stream()
            .map(it -> it.source() + " -> " + it.target() + (it.isRshared() ? " (R_SHARED)" : ""))
            .collect(Collectors.joining("\n\t")));

        try {
            Thread.sleep(300_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final List<Mount> dockerMounts = new ArrayList<>();
        config.mounts().forEach(m -> {
            var mount = new Mount().withType(MountType.BIND).withSource(m.source()).withTarget(m.target());
            /*
            if (m.isRshared()) {
                mount.withBindOptions(new BindOptions().withPropagation(BindPropagation.R_SHARED));
            }
            */
            dockerMounts.add(mount);
        });

        final HostConfig hostConfig = new HostConfig();
        hostConfig.withMounts(dockerMounts);

        final CreateContainerCmd createContainerCmd = DOCKER.createContainerCmd(sourceImage)
            .withHostConfig(hostConfig)
            .withAttachStdout(true)
            .withAttachStderr(true);

        container = createContainerCmd
            .withTty(true)
            .exec();
        LOG.info("Creating container from image={} done, id={}", sourceImage, container.getId());

        LOG.info("Starting env container with id {} ...", container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();
        LOG.info("Starting env container with id {} done", container.getId());
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

        final CompletableFuture<Long> exitCode = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            try {
                LOG.info("Creating cmd {}", String.join(" ", command));
                final ExecCreateCmd execCmd = DOCKER.execCreateCmd(container.getId())
                    .withCmd(command)
                    .withAttachStdout(true)
                    .withAttachStderr(true);
                if (envp != null && envp.length > 0) {
                    execCmd.withEnv(List.of(envp));
                }
                final ExecCreateCmdResponse exec = execCmd.exec();
                LOG.info("Executing cmd {}", String.join(" ", command));
                DOCKER.execStartCmd(exec.getId())
                    .exec(new ResultCallbackTemplate<>() {
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
                    }).awaitCompletion();
                LOG.info("Closing stdout, stderr of cmd {}", String.join(" ", command));
                stdout.close();
                stderr.close();
                exitCode.complete(DOCKER.inspectExecCmd(exec.getId()).exec().getExitCodeLong());
            } catch (InterruptedException | IOException e) {
                LOG.error("Job container with id=" + container.getId() + " image= " + sourceImage
                    + " was interrupted");
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
            public int waitFor() {
                try {
                    return exitCode.get().intValue();
                } catch (InterruptedException e) {
                    LOG.error("LzyProcess was interrupted, failed to get exit code", e);
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    LOG.error("LzyProcess was failed with ExecutionException, failed to get exit code", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void signal(int sigValue) {
                DOCKER.killContainerCmd(container.getId()) // TODO(d-kruchinin): execId?
                    .withSignal(String.valueOf(sigValue))
                    .exec();
            }
        };
    }

    @Override
    public void close() throws Exception {
        DOCKER.killContainerCmd(container.getId()).exec();
    }

    private String prepareImage(BaseEnvConfig config) {
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
