package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.apache.commons.io.IOUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        final Process process;
        try {
            process = Utils.javaProcess(LzyServant.class.getCanonicalName(), lzyArgs).inheritIO().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servantProcesses.add(process);
        return new Servant() {
            @Override
            public boolean isAlive() {
                return process.isAlive();
            }

            @Override
            public boolean pathExists(Path path) {
                return Files.exists(path);
            }

            @Override
            public ExecutionResult execute(String... command) {
                return null;
            }

            @Override
            public boolean waitForStatus(ServantStatus status, long timeout, TimeUnit unit) {
                return Utils.waitFlagUp(() -> {
                    if (pathExists(Paths.get(path + "/sbin/status"))) {
                        try {
                            final Process bash = new ProcessBuilder("bash", path + "/sbin/status").start();
                            bash.waitFor();
                            final String stdout = IOUtils.toString(bash.getInputStream(), StandardCharsets.UTF_8);
                            final String parsedStatus = stdout.split("\n")[0];
                            return parsedStatus.toLowerCase().equals(status.name().toLowerCase());
                        } catch (InterruptedException | IOException e) {
                            return false;
                        }
                    }
                    return false;
                }, timeout, unit);
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
