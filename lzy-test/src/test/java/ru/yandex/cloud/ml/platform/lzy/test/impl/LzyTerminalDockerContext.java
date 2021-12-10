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
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.testcontainers.shaded.org.apache.commons.lang.SystemUtils.IS_OS_LINUX;

public class LzyTerminalDockerContext implements LzyTerminalTestContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(LzyTerminalDockerContext.class);
    private final List<GenericContainer<?>> startedContainers = new ArrayList<>();

    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, String serverAddress, int debugPort, String user, String private_key_path) {
        final String internalHost = IS_OS_LINUX ? "localhost" : "host.docker.internal";
        final String uuid = UUID.randomUUID().toString().substring(0, 5);
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but it is easier for testing
            .withEnv("USER", user)
            .withEnv("LOG_FILE", "terminal_" + uuid)
            .withEnv("DEBUG_PORT", Integer.toString(debugPort))
            .withEnv("SUSPEND_DOCKER", "n")
            //.withFileSystemBind("/var/log/servant/", "/var/log/servant/")
            .withCommand("--lzy-address " + serverAddress + " "
                + "--host localhost "
                + "--port " + port + " "
                + "--lzy-mount " + mount + " "
                + "--internal-host " + internalHost + " "
                + "--private-key " + private_key_path + " "
                + "terminal");

        if (private_key_path != null) {
            base.withFileSystemBind(private_key_path, private_key_path);
        }

        final GenericContainer<?> servantContainer;
        if (SystemUtils.IS_OS_LINUX) {
            servantContainer = base.withNetworkMode("host");
        } else {
            servantContainer = base
                .withFixedExposedPort(port, port)
                .withFixedExposedPort(debugPort, debugPort) //to attach debugger
                .withExposedPorts(port, debugPort);
        }

        servantContainer.start();
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        startedContainers.add(servantContainer);
        return new Terminal() {
            @Override
            public String mount() {
                return mount;
            }

            @Override
            public String serverAddress() {
                return serverAddress;
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

                    final ToStringConsumer stdoutStringConsumer = new ToStringConsumer();
                    final ToStringConsumer stderrStringConsumer = new ToStringConsumer();
                    final Slf4jLogConsumer slf4jLogConsumer = new Slf4jLogConsumer(LOGGER);

                    final MultiLogsConsumer stdoutConsumer = new MultiLogsConsumer(stdoutStringConsumer, slf4jLogConsumer);
                    final MultiLogsConsumer stderrConsumer = new MultiLogsConsumer(stderrStringConsumer, slf4jLogConsumer);

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
                            return stdoutStringConsumer.toString(StandardCharsets.UTF_8);
                        }

                        @Override
                        public String stderr() {
                            return stderrStringConsumer.toString(StandardCharsets.UTF_8);
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
                AgentStatus status, long timeout, TimeUnit unit
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
