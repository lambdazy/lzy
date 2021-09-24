package ru.yandex.cloud.ml.platform.lzy.test.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.exception.ConflictException;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.testcontainers.shaded.org.apache.commons.lang.SystemUtils.IS_OS_LINUX;

public class LzyServantDockerContext implements LzyServantTestContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(LzyServantDockerContext.class);
    private final List<GenericContainer<?>> startedContainers = new ArrayList<>();

    @Override
    public Servant startTerminalAtPathAndPort(String mount, int port, String serverHost, int serverPort) {
        final String internalHost = IS_OS_LINUX ? "localhost" : "host.docker.internal";
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but it is easier for testing
            .withEnv("USER", "terminal-test")
            .withEnv("LOG_FILE", "terminal")
            .withEnv("DEBUG_PORT", "5006")
            .withEnv("SUSPEND_DOCKER", "n")
            //.withFileSystemBind("/tmp/lzy/run/", "/var/log/servant/")
            .withCommand("--lzy-address " + serverHost + ":" + serverPort + " "
                + "--host localhost "
                + "--port " + port + " "
                + "--lzy-mount " + mount + " "
                + "--internal-host " + internalHost + " "
                + "terminal");

        final GenericContainer<?> servantContainer;
        if (SystemUtils.IS_OS_LINUX) {
            servantContainer = base.withNetworkMode("host");
        } else {
            servantContainer = base
                .withFixedExposedPort(port, port)
                .withFixedExposedPort(5006, 5006) //to attach debugger
                .withExposedPorts(port, 5006);
        }

        servantContainer.start();
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        startedContainers.add(servantContainer);
        return new Servant() {
            @Override
            public String mount() {
                return mount;
            }

            @Override
            public String serverHost() {
                return serverHost;
            }

            @Override
            public int port() {
                return port;
            }

            @Override
            public boolean pathExists(Path path) {
                try {
                    final Container.ExecResult ls = servantContainer.execInContainer("ls", path.toString());
                    return ls.getExitCode() == 0;
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ExecutionResult execute(Map<String, String> env, String... command) {
                try {
                    final String containerId = servantContainer.getContainerInfo().getId();
                    final DockerClient dockerClient = DockerClientFactory.instance().client();

                    final ExecCreateCmd execCreateCmd = dockerClient.execCreateCmd(containerId);
                    final ExecCreateCmdResponse exec = execCreateCmd.withEnv(env.entrySet()
                            .stream()
                            .map(e -> e.getKey() + "=" + Utils.bashEscape(e.getValue()))
                            .collect(Collectors.toList()))
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .withCmd(command)
                        .exec();

                    final ToStringConsumer stdoutConsumer = new ToStringConsumer();
                    final ToStringConsumer stderrConsumer = new ToStringConsumer();

                    try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
                        callback.addConsumer(OutputFrame.OutputType.STDOUT, stdoutConsumer);
                        callback.addConsumer(OutputFrame.OutputType.STDERR, stderrConsumer);
                        dockerClient.execStartCmd(exec.getId()).exec(callback).awaitCompletion();
                    }
                    //noinspection deprecation
                    final int exitCode = dockerClient.inspectExecCmd(exec.getId()).exec().getExitCode();

                    return new ExecutionResult() {
                        @Override
                        public String stdout() {
                            return stdoutConsumer.toString(StandardCharsets.UTF_8);
                        }

                        @Override
                        public String stderr() {
                            return stderrConsumer.toString(StandardCharsets.UTF_8);
                        }

                        @Override
                        public int exitCode() {
                            return exitCode;
                        }
                    };
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean waitForStatus(
                ServantStatus status, long timeout, TimeUnit unit
            ) {
                return Utils.waitFlagUp(() -> {
                    if (pathExists(Paths.get(mount + "/sbin/status"))) {
                        try {
                            final Container.ExecResult bash = servantContainer.execInContainer(
                                "bash",
                                mount + "/sbin/status"
                            );
                            final String parsedStatus = bash.getStdout().split("\n")[0];
                            return parsedStatus.equalsIgnoreCase(status.name());
                        } catch (InterruptedException | IOException e) {
                            return false;
                        }
                    }
                    return false;
                }, timeout, unit);
            }

            @Override
            public boolean waitForShutdown(long timeout, TimeUnit unit) {
                return Utils.waitFlagUp(() -> {
                    try {
                        servantContainer.execInContainer("bash");
                    } catch (ConflictException ce) {
                        if (ce.getHttpStatus() == 409) { //not running code
                            return true;
                        }
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }, timeout, unit);
            }
        };
    }

    @Override
    public boolean inDocker() {
        return true;
    }

    @Override
    public void close() {
        startedContainers.forEach(GenericContainer::stop);
    }
}
