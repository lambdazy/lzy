package ru.yandex.cloud.ml.platform.lzy.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface LzyTerminalTestContext extends AutoCloseable {
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    Logger LOGGER = LoggerFactory.getLogger(LzyTerminalTestContext.class);
    int DEFAULT_TIMEOUT_SEC = 30;
    String TEST_USER = "test-user";

    default Terminal startTerminalAtPathAndPort(String path, int port, String serverAddress) {
        return startTerminalAtPathAndPort(path, port, serverAddress, 5006, TEST_USER, null);
    }
    Terminal startTerminalAtPathAndPort(String path, int port, String serverAddress, int debugPort, String user, String privateKeyPath);

    boolean inDocker();
    void close();

    interface Terminal {
        boolean pathExists(Path path);
        String mount();
        @SuppressWarnings("unused")
        int port();
        String serverAddress();

        @SuppressWarnings("UnusedReturnValue")
        default ExecutionResult execute(String... command) {
            return execute(Collections.emptyMap(), command);
        }

        ExecutionResult execute(Map<String, String> env, String... command);

        default ExecutionResult run(String zygoteName, String arguments, Map<String, String> bindings) {
            return run(zygoteName, arguments, bindings, Map.of());
        }

        default ExecutionResult run(String zygoteName, String arguments, Map<String, String> bindings, Map<String, String> mappings) {
            try {
                final ExecutionResult bash = execute(
                        Collections.emptyMap(),
                        "bash",
                        "-c",
                        "echo '" + OBJECT_MAPPER.writeValueAsString(bindings) + "' > bindings.json"
                );
                System.out.println(bash);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            try {
                final ExecutionResult bash = execute(
                        Collections.emptyMap(),
                        "bash",
                        "-c",
                        "echo '" + OBJECT_MAPPER.writeValueAsString(mappings) + "' > mapping.json"
                );
                System.out.println(bash);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            final ExecutionResult execute = execute(
                    Collections.emptyMap(),
                    "/bin/bash",
                    "-c",
                    String.join(
                            " ",
                            mount() + "/bin/" + zygoteName,
                            "-m",
                            "bindings.json",
                            "-s",
                            "mapping.json",
                            arguments
                    )
            );
            LOGGER.info("\u001B[31m\nEXECUTED COMMAND: {}\u001B[30m", zygoteName);
            LOGGER.info("Stdout: {}", execute.stdout());
            LOGGER.info("Stderr: {}", execute.stderr());
            LOGGER.info("Exit code: {}", execute.exitCode());
            return execute;
        }

        default void publish(String zygoteName, AtomicZygote zygote) {
            try {
                execute(
                    Collections.emptyMap(),
                    "bash",
                    "-c",
                    "echo '" + JsonFormat.printer().print(gRPCConverter.to(zygote)) + "' > filename"
                );
                execute(
                    Collections.emptyMap(),
                    "bash",
                    "-c",
                    String.join(
                        " ",
                        mount() + "/sbin/publish",
                        zygoteName,
                        "filename",
                        "-z",
                        serverAddress() // serverAddress
                    )
                );
                Utils.waitFlagUp(
                    () -> pathExists(Path.of(mount() + "/bin/" + zygoteName)),
                    30,
                    TimeUnit.SECONDS
                );
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }

        default void createChannel(String channelName) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                String.join(
                    " ",
                    mount() + "/sbin/channel",
                    "create",
                    channelName
                )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
        }

        default String getWhiteboard(String wbId) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/whiteboard",
                            "get",
                            wbId
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            return execute.stdout();
        }

        default String getAllWhiteboards() {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/whiteboard",
                            "getAll"
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            return execute.stdout();
        }

        default String createSnapshot() {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/snapshot",
                            "create"
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            return execute.stdout();
        }

        default String createWhiteboard(String wbId, List<String> fieldNames) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/whiteboard",
                            "create",
                            wbId,
                            "-l",
                            String.join(",", fieldNames)
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            return execute.stdout();
        }

        default void link(String wbId, String fieldId, String entryId) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/whiteboard",
                            "link",
                            wbId,
                            "-f",
                            fieldId,
                            "-e",
                            entryId
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            execute.stdout();
        }

        default void finalizeSnapshot(String spId) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/snapshot",
                            "finalize",
                            spId
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
        }

        default void destroyChannel(String channelName) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                    String.join(
                            " ",
                            mount() + "/sbin/channel",
                            "destroy",
                            channelName
                    )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
        }

        default String channelStatus(String channelName) {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                String.join(
                    " ",
                    mount() + "/sbin/channel",
                    "status",
                    channelName
                )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            return execute.stdout();
        }

        default void createSlot(String path, String channelName, Slot slot) {
            try {
                execute(
                    Collections.emptyMap(),
                    "bash",
                    "-c",
                    "echo '" + JsonFormat.printer().print(gRPCConverter.to(slot)) + "' > slot.json"
                );
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            final ExecutionResult execute = execute(
                Collections.emptyMap(),
                "bash",
                "-c",
                String.join(
                    " ",
                    mount() + "/sbin/touch",
                    path,
                    channelName,
                    "--slot",
                    "slot.json"
                )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
            if (slot.direction() == Slot.Direction.OUTPUT) {
                Utils.waitFlagUp(() -> pathExists(Path.of(path)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
            }
        }

        default void update() {
            final ExecutionResult execute = execute(Collections.emptyMap(), "bash", "-c",
                String.join(
                    " ",
                    mount() + "/sbin/update"
                )
            );
            if (execute.exitCode() != 0) {
                throw new RuntimeException(execute.stderr());
            }
        }

        boolean waitForStatus(AgentStatus status, long timeout, TimeUnit unit);
        boolean waitForShutdown(long timeout, TimeUnit unit);

        interface ExecutionResult {
            String stdout();
            String stderr();
            int exitCode();
        }
    }
}
