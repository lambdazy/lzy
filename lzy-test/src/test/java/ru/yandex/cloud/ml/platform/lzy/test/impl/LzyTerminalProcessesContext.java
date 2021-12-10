package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.apache.commons.io.IOUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.BashApi;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.apache.commons.lang.SystemUtils.IS_OS_LINUX;

public class LzyTerminalProcessesContext implements LzyTerminalTestContext {
    private final List<Process> servantProcesses = new ArrayList<>();

    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, String serverAddress, int debugPort, String user, String privateKeyPath) {
        final String[] lzyArgs = {
            "--lzy-address",
            serverAddress,
            "--host",
            "localhost",
            "--port",
            String.valueOf(port),
            "--lzy-mount",
            mount,
            "--private-key",
            privateKeyPath != null ? privateKeyPath : "/tmp/nonexistent_key",
            "terminal"
        };
        final String pathServantLog4jFile =
            Path.of(System.getProperty("user.dir")).getParent() +
                "/lzy-servant/src/main/resources/log4j2.yaml";
        final String[] systemArgs = {
            "-Djava.library.path=/usr/local/lib",
            "-Dlog4j.configurationFile=" + pathServantLog4jFile,
            "-Dcustom.log.file=terminal.log",
            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:" + debugPort
        };
        final Process process;
        try {
            process = Utils.javaProcess(BashApi.class.getCanonicalName(), lzyArgs, systemArgs).inheritIO().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servantProcesses.add(process);
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
            public boolean waitForStatus(AgentStatus status, long timeout, TimeUnit unit) {
                return Utils.waitFlagUp(() -> {
                    if (pathExists(Paths.get(mount + "/sbin/status"))) {
                        try {
                            final Process bash = new ProcessBuilder("bash", mount + "/sbin/status").start();
                            bash.waitFor();
                            final String stdout = IOUtils.toString(bash.getInputStream(), StandardCharsets.UTF_8);
                            final String parsedStatus = stdout.split("\n")[0];
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
