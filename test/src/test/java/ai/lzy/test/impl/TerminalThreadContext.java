package ai.lzy.test.impl;

import ai.lzy.test.LzyTerminalTestContext;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import ai.lzy.model.utils.JwtCredentials;
import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyTerminal;

import javax.annotation.Nullable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TerminalThreadContext implements LzyTerminalTestContext {

    private final Map<String, LzyTerminal> terminals = new HashMap<>();

    @Override
    public Terminal startTerminalAtPathAndPort(
        String path,
        int port,
        int fsPort,
        String serverAddress,
        String channelManagerAddress,
        int debugPort,
        String user,
        String privateKeyPath
    ) {
        if (terminals.get(path) != null) {
            final LzyTerminal term = terminals.remove(path);
            term.close();
            try {
                term.awaitTermination();
            } catch (InterruptedException | IOException e) {
                LOGGER.error(e);
            }
        }
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
            .channelManagerAddress(URI.create(channelManagerAddress))
            .user(user)
            .agentHost("localhost")
            .agentPort(port)
            .fsPort(fsPort)
            .root(Path.of(path))
            .token(token)
            .build();
        final LzyTerminal terminal;
        try {
            terminal = new LzyTerminal(config);
            terminals.put(path, terminal);
        } catch (URISyntaxException
            | IOException
            | InvocationTargetException
            | NoSuchMethodException
            | InstantiationException
            | IllegalAccessException e
        ) {
            throw new RuntimeException(e);
        }

        return new Terminal() {
            @Override
            public String mount() {
                return path;
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

            @Nullable
            @Override
            public AgentStatus status() {
                if (pathExists(Paths.get(path + "/sbin/status"))) {
                    try {
                        final Process bash = new ProcessBuilder("bash", path + "/sbin/status").start();
                        bash.waitFor();
                        final String stdout = IOUtils.toString(bash.getInputStream(), StandardCharsets.UTF_8);
                        final String parsedStatus = stdout.split("\n")[0];
                        return AgentStatus.valueOf(parsedStatus);
                    } catch (IllegalArgumentException | InterruptedException | IOException e) {
                        return null;
                    }
                }
                return null;
            }

            @Override
            public boolean waitForShutdown() {
                try {
                    terminal.awaitTermination();
                    return true;
                } catch (InterruptedException | IOException e) {
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
    public void close() {
        for (Map.Entry<String, LzyTerminal> terminalEntry : terminals.entrySet()) {
            terminalEntry.getValue().close();
            try {
                terminalEntry.getValue().awaitTermination();
                if (SystemUtils.IS_OS_MAC) {
                    Runtime.getRuntime().exec(new String[]{"umount", "-f", terminalEntry.getKey()});
                } else {
                    Runtime.getRuntime().exec(new String[]{"umount", terminalEntry.getKey()});
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
