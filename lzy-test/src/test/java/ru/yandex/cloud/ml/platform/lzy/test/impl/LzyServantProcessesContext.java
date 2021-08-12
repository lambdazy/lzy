package ru.yandex.cloud.ml.platform.lzy.test.impl;

import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LzyServantProcessesContext implements LzyServantTestContext {
    private final List<Process> servantProcesses = new ArrayList<>();

    @Override
    public Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort) {
        final String[] lzyArgs = {
            "--lzy-address",
            serverHost + ":" + serverPort,
            "--host",
            "localhost",
            "--port",
            String.valueOf(port),
            "--lzy-mount",
            path,
            "--private-key",
            "/tmp/nonexistent-key",
            "terminal"
            };
        final List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin" + "/java");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LzyServant.class.getCanonicalName());
        command.addAll(Arrays.asList(lzyArgs));

        final ProcessBuilder builder = new ProcessBuilder(command);
        final Process process;
        try {
            process = builder.inheritIO().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servantProcesses.add(process);
        return new Servant() {
            @Override
            public boolean pathExists(Path path) {
                return Files.exists(path);
            }

            @Override
            public ExecutionResult execute(String... command) {
                return null;
            }
        };
    }

    @Override
    public boolean inDocker() {
        return false;
    }

    @Override
    public void close() {
        servantProcesses.forEach(Process::destroy);
    }
}
