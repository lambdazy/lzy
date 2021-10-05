package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.apache.commons.io.IOUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LzyServantProcessesContext implements LzyServantTestContext {
    private final List<Process> servantProcesses = new ArrayList<>();

    @Override
    public Servant startTerminalAtPathAndPort(String mount, int port, String serverHost, int serverPort) {
        final String[] lzyArgs = {
            "--lzy-address",
            serverHost + ":" + serverPort,
            "--host",
            "localhost",
            "--port",
            String.valueOf(port),
            "--lzy-mount",
            mount,
            "--private-key",
            "/tmp/nonexistent-key",
            "--internal-host",
            "localhost",
            "terminal"
        };
        final String pathServantLog4jFile =
            Path.of(System.getProperty("user.dir")).getParent() +
                "/lzy-servant/src/main/resources/log4j2.yaml";
        final String[] systemArgs = {
            "-Djava.library.path=/usr/local/lib",
            "-Dlog4j.configurationFile=" + pathServantLog4jFile,
            "-Dcustom.log.file=terminal.log"//,
            //"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5056"
        };
        final Process process;
        try {
            process = Utils.javaProcess(LzyServant.class.getCanonicalName(), lzyArgs, systemArgs).inheritIO().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servantProcesses.add(process);
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
                return Files.exists(path);
            }

            @Override
            public ExecutionResult execute(Map<String, String> env, String... command) {
                try {
                    final Process exec = Runtime.getRuntime().exec(command);
                    final OutputStreamWriter stdin = new OutputStreamWriter(
                        exec.getOutputStream(),
                        StandardCharsets.UTF_8
                    );
                    stdin.close();

                    final String stdout = IOUtils.toString(exec.getInputStream(), StandardCharsets.UTF_8);
                    final String stderr = IOUtils.toString(exec.getErrorStream(), StandardCharsets.UTF_8);
                    final int rc = exec.waitFor();
                    return new ExecutionResult() {
                        @Override
                        public String stdout() {
                            return stdout;
                        }

                        @Override
                        public String stderr() {
                            return stderr;
                        }

                        @Override
                        public int exitCode() {
                            return rc;
                        }
                    };
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean waitForStatus(ServantStatus status, long timeout, TimeUnit unit) {
                return Utils.waitFlagUp(() -> {
                    if (pathExists(Paths.get(mount + "/sbin/status"))) {
                        try {
                            final Process bash = new ProcessBuilder("bash", mount + "/sbin/status").start();
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

            @Override
            public boolean waitForShutdown(long timeout, TimeUnit unit) {
                return Utils.waitFlagUp(() -> !process.isAlive(), timeout, unit);
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
