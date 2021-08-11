package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LzyServantDockerContext implements LzyServantTestContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(LzyServantDockerContext.class);
    private final List<GenericContainer<?>> startedContainers = new ArrayList<>();

    @Override
    public Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort) {
        //noinspection deprecation
        final GenericContainer<?> servantContainer = new FixedHostPortGenericContainer<>("lzy-servant")
            .withFixedExposedPort(port, port)
            .withPrivilegedMode(true) //it is not necessary to use privileged mode for FUSE, but is is easier for testing
            .withEnv("USER", "terminal-test")
            .withCommand("terminal "
                + "--lzy-address " + serverHost + ":" + serverPort + " "
                + "--host localhost "
                + "--port " + port + " "
                + "--lzy-mount " + path)
            .withExposedPorts(port);
        servantContainer.start();
        servantContainer.followOutput(new Slf4jLogConsumer(LOGGER));
        startedContainers.add(servantContainer);
        return new Servant() {
            @Override
            public boolean pathExists(Path path) {
                return false;
            }

            @Override
            public String execute(String... command) {
                return null;
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
