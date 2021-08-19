package ru.yandex.cloud.ml.platform.lzy.test.impl;

import com.github.dockerjava.api.exception.ConflictException;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LzyServantDockerContext implements LzyServantTestContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(LzyServantDockerContext.class);
    private final List<GenericContainer<?>> startedContainers = new ArrayList<>();

    @Override
    public Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort) {
        //noinspection deprecation
        final FixedHostPortGenericContainer<?> base = new FixedHostPortGenericContainer<>("lzy-servant")
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but is is easier for testing
            .withEnv("USER", "terminal-test")
            .withCommand("--lzy-address " + serverHost + ":" + serverPort + " "
                + "--host localhost "
                + "--port " + port + " "
                + "--lzy-mount " + path + " "
                + "terminal");

        final GenericContainer<?> servantContainer;
        if (SystemUtils.IS_OS_LINUX) {
            servantContainer = base.withNetworkMode("host");
        } else {
            servantContainer = base
                .withFixedExposedPort(port, port)
                .withFixedExposedPort(5005, 5005) //to attach debugger
                .withExposedPorts(port, 5005);
        }

        servantContainer.start();
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        startedContainers.add(servantContainer);
        return new Servant() {
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
            public ExecutionResult execute(String... command) {
                try {
                    final Container.ExecResult execResult = servantContainer.execInContainer(command);
                    return new ExecutionResult() {
                        @Override
                        public String stdout() {
                            return execResult.getStdout();
                        }

                        @Override
                        public String stderr() {
                            return execResult.getStderr();
                        }

                        @Override
                        public int exitCode() {
                            return execResult.getExitCode();
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
                    if (pathExists(Paths.get(path + "/sbin/status"))) {
                        try {
                            final Container.ExecResult bash = servantContainer.execInContainer(
                                "bash",
                                path + "/sbin/status"
                            );
                            final String parsedStatus = bash.getStdout().split("\n")[0];
                            return parsedStatus.toLowerCase().equals(status.name().toLowerCase());
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
