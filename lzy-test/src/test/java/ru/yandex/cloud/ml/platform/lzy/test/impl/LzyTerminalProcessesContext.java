package ru.yandex.cloud.ml.platform.lzy.test.impl;

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

import org.apache.commons.io.IOUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.BashApi;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import javax.annotation.Nullable;

public class LzyTerminalProcessesContext implements LzyTerminalTestContext {
    private final List<Process> servantProcesses = new ArrayList<>();

    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, int fsPort, String serverAddress, int debugPort,
                                               String user, String privateKeyPath) {
        final String[] lzyArgs = {
            "--lzy-address",
            serverAddress,
            "--host",
            "localhost",
            "--port",
            String.valueOf(port),
            "--fs-port",
            String.valueOf(fsPort),
            "--lzy-mount",
            mount,
            "--private-key",
            privateKeyPath,
            "terminal"
        };
        final String pathServantLog4jFile =
            Path.of(System.getProperty("user.dir")).getParent() +
                "/lzy-servant/src/main/resources/log4j2.yaml";
        final String pathServantCmdLog4jFile =
                Path.of(System.getProperty("user.dir")).getParent() +
                        "/lzy-servant/src/main/resources/cmd_config_log4j2.yaml";
        final String[] systemArgs = {
            "-Djava.library.path=/usr/local/lib",
            "-Dlog4j.configurationFile=" + pathServantLog4jFile,
            "-Dcmd.log4j.configurationFile=" + pathServantCmdLog4jFile,
            "-Dcustom.log.file=terminal.log",
            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:" + debugPort
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
            public int fsPort() {
                return fsPort;
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

            @Nullable
            @Override
            public AgentStatus status() {
                if (pathExists(Paths.get(mount + "/sbin/status"))) {
                    try {
                        final Process bash = new ProcessBuilder("bash", mount + "/sbin/status").start();
                        bash.waitFor();
                        final String stdout = IOUtils.toString(bash.getInputStream(), StandardCharsets.UTF_8);
                        final String parsedStatus = stdout.split("\n")[0];
                        return AgentStatus.valueOf(parsedStatus);
                    } catch (InterruptedException | IOException e) {
                        return null;
                    }
                }
                return null;
            }

            @Override
            public boolean waitForShutdown() {
                return Utils.waitFlagUp(() -> !process.isAlive(), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
            }

            @Override
            public void shutdownNow() {
                if (process.isAlive()) {
                    process.destroy();
                }
            }
        };
    }

    @Override
    public void close() {
        servantProcesses.forEach(Process::destroy);
    }
}
