package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyAgentConfig;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyTerminal;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TerminalThreadContext implements LzyTerminalTestContext {
    LzyTerminal terminal;
    String mount;

    @Override
    public Terminal startTerminalAtPathAndPort(String path, int port, String serverAddress, int debugPort, String user, String privateKeyPath) {
        mount = path;

        final String pathServantLog4jFile =
                Path.of(System.getProperty("user.dir")).getParent() +
                        "/lzy-servant/src/main/resources/log4j2.yaml";
        final String pathServantCmdLog4jFile =
                Path.of(System.getProperty("user.dir")).getParent() +
                        "/lzy-servant/src/main/resources/cmd_config_log4j2.yaml";

        System.setProperty("log4j.configurationFile", pathServantLog4jFile);
        System.setProperty("cmd.log4j.configurationFile", pathServantCmdLog4jFile);
        System.setProperty("custom.log.file", "/tmp/lzy_servant.log");
        final String token;
        if (privateKeyPath == null) {
            token = "";
        } else {
            try (final FileReader reader = new FileReader(privateKeyPath)) {
                token = JwtCredentials.buildJWT(user, reader);
            } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }
        }

        final LzyAgentConfig config = LzyAgentConfig.builder()
            .serverAddress(URI.create(serverAddress))
            .whiteboardAddress(URI.create(serverAddress))
            .user(user)
            .agentName("localhost")
            .agentInternalName("localhost")
            .agentPort(port)
            .root(Path.of(path))
            .token(token)
            .build();
        try {
            terminal = new LzyTerminal(config);
            terminal.start();
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }

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
                    var stream = Stream.concat(
                        System.getenv().entrySet().stream(),
                        env.entrySet().stream()
                    );
                    final Process exec = Runtime.getRuntime().exec(command, stream
                        .map(e -> e.getKey() + "=" + Utils.bashEscape(e.getValue()))
                        .toArray(String[]::new));
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
                try {
                    terminal.awaitTermination();
                    return true;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void shutdownNow() {
                terminal.close();
            }
        };
    }

    @Override
    public boolean inDocker() {
        return false;
    }

    @Override
    public void close() {
        if (terminal == null)
            return;

        terminal.close();
        try {
            terminal.awaitTermination();
            if (SystemUtils.IS_OS_MAC) {
                Runtime.getRuntime().exec("umount -f " + mount);
            } else {
                Runtime.getRuntime().exec("umount " + mount);
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
