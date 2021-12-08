package ru.yandex.cloud.ml.platform.lzy.servant.env;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.BindOptions;
import com.github.dockerjava.api.model.BindPropagation;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.core.DockerClientBuilder;
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
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecutionException;

public class BaseEnv implements Environment {

    private static final Logger LOG = LogManager.getLogger(BaseEnv.class);
    private static final DockerClient DOCKER = DockerClientBuilder.getInstance().build();

    public final CreateContainerResponse container;

    public String defaultImage() {
        return "celdwind/lzy:default-env";
    }

    public BaseEnv(EnvConfig config) {
        LOG.info("Creating container: image={}, ", defaultImage());
        LOG.info("Mount options: {}", config.mounts.toString());
        final List<Mount> dockerMounts = new ArrayList<>();
        dockerMounts.add(
            new Mount()
                .withSource("/tmp/servant/lzy")
                .withTarget("/tmp/lzy")
                .withType(MountType.BIND)
                .withBindOptions(new BindOptions().withPropagation(BindPropagation.R_SHARED))
        );
        config.mounts.forEach(m -> dockerMounts.add(
            new Mount()
                .withType(MountType.BIND)
                .withSource(m.source)
                .withTarget(m.target)
        ));

        final HostConfig hostConfig = new HostConfig();
        hostConfig.withMounts(dockerMounts);

        final CreateContainerCmd createContainerCmd = DOCKER.createContainerCmd(defaultImage())
            .withHostConfig(hostConfig)
            .withAttachStdout(true)
            .withAttachStderr(true);

        container = createContainerCmd
            .withTty(true)
            .exec();
        LOG.info("Created container id = {}", container.getId());

        LOG.info("Starting container with id = " + container.getId());
        DOCKER.startContainerCmd(container.getId()).exec();
        LOG.info("Started container with id = " + container.getId());
    }

    @Override
    public LzyProcess runProcess(String... command)
        throws EnvironmentInstallationException, LzyExecutionException {

        final int bufferSize = 1024;
        final PipedInputStream stdoutPipe = new PipedInputStream(bufferSize);
        final PipedInputStream stderrPipe = new PipedInputStream(bufferSize);
        final PipedOutputStream stdout;
        final PipedOutputStream stderr;
        try {
            stdout = new PipedOutputStream(stdoutPipe);
            stderr = new PipedOutputStream(stderrPipe);
        } catch (IOException e) {
            throw new LzyExecutionException(e);
        }

        final CompletableFuture<Long> exitCode = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            try {
                LOG.info("Creating cmd {}", String.join(" ", command));
                final ExecCreateCmdResponse exec = DOCKER.execCreateCmd(container.getId())
                    .withCmd(command)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .exec();
                LOG.info("Executing cmd {}", String.join(" ", command));
                DOCKER.execStartCmd(exec.getId())
                    .exec(new ResultCallbackTemplate<>() {
                        @Override
                        public void onNext(Frame item) {
                            switch (item.getStreamType()) {
                                case STDOUT:
                                    try {
                                        LOG.info(
                                            "attempt to write #{} bytes str:{}",
                                            item.getPayload().length,
                                            new String(item.getPayload(), StandardCharsets.UTF_8)
                                        );
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
                                    LOG.info("Got frame " +
                                        new String(item.getPayload(), StandardCharsets.UTF_8) +
                                        " from unknown stream type " +
                                        item.getStreamType());
                            }
                        }
                    }).awaitCompletion();
                LOG.info("Closing stdout, stderr of cmd {}", String.join(" ", command));
                stdout.close();
                stderr.close();
                exitCode.complete(DOCKER.inspectExecCmd(exec.getId()).exec().getExitCodeLong());
            } catch (InterruptedException | IOException e) {
                LOG.error("Job container with id=" + container.getId() + " image= " + defaultImage()
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
                } catch (InterruptedException | ExecutionException e) {
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
}
