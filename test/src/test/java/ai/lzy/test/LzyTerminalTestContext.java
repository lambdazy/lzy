package ai.lzy.test;

import ai.lzy.fs.commands.BuiltinCommandHolder;
import ai.lzy.fs.commands.CommandHolder;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.deprecated.AtomicZygote;
import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.servant.commands.ServantCommandHolder;
import ai.lzy.v1.ChannelManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public interface LzyTerminalTestContext extends AutoCloseable {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    Logger LOGGER = LogManager.getLogger(LzyTerminalTestContext.class);
    int DEFAULT_TIMEOUT_SEC = 30;
    String TEST_USER = "test-user";

    Terminal startTerminalAtPathAndPort(
        String path,
        int port,
        int fsPort,
        String serverAddress,
        String channelManagerAddress,
        int debugPort,
        String user,
        String privateKeyPath);

    void close();

    class TerminalCommandFailedException extends RuntimeException {
        private final Terminal.ExecutionResult result;

        public TerminalCommandFailedException(Terminal.ExecutionResult result) {
            super(result.stderr());
            this.result = result;
        }

        public Terminal.ExecutionResult getResult() {
            return result;
        }
    }

    interface Terminal {

        boolean pathExists(Path path);

        String mount();

        @SuppressWarnings("unused")
        int port();

        int fsPort();

        String serverAddress();

        @SuppressWarnings("UnusedReturnValue")
        default ExecutionResult executeTerminalCommand(Path binaryPath, String... command) {
            TimeUtils.waitFlagUp(() -> pathExists(binaryPath), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
            final ExecutionResult result = execute(
                binaryPath.toString(),
                String.join(" ", command)
            );
            LOGGER.info("STDOUT:\n{}", result.stdout());
            LOGGER.info("STDERR:\n{}", result.stderr());
            if (result.exitCode() != 0) {
                LOGGER.error("Command {}/{} execution failed with error {}", binaryPath, command, result.stderr());
                throw new TerminalCommandFailedException(result);
            }
            return result;
        }

        default ExecutionResult execute(String... command) {
            return execute(Collections.emptyMap(), "/bin/bash", "-c", String.join(" ", command));
        }

        ExecutionResult execute(Map<String, String> env, String... command);

        default ExecutionResult executeLzyCommand(CommandHolder commandName, String... args) {
            return executeTerminalCommand(Path.of(mount() + "/sbin/" + commandName.toString()), args);
        }

        default ExecutionResult run(String zygoteName, String arguments, Map<String, String> bindings) {
            try {
                final ExecutionResult resultEcho = execute(
                    "echo '" + OBJECT_MAPPER.writeValueAsString(bindings) + "' > bindings.json"
                );
                System.out.println(resultEcho);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            final ExecutionResult result = execute(
                mount() + "/bin/" + zygoteName,
                "-m",
                "bindings.json",
                arguments
            );
            LOGGER.info("\u001B[31m\nEXECUTED COMMAND: {}\u001B[0m", zygoteName);
            LOGGER.info("Stdout: {}", result.stdout());
            LOGGER.info("Stderr: {}", result.stderr());
            LOGGER.info("Exit code: {}", result.exitCode());
            return result;
        }

        default String tasksStatus() {
            final ExecutionResult execute = executeLzyCommand(
                ServantCommandHolder.ts,
                "filename",
                "-z",
                serverAddress() // serverAddress
            );
            return execute.stdout();
        }

        default void publish(AtomicZygote zygote) {
            try {
                execute("echo '" + JsonFormat.printer().print(GrpcConverter.to(zygote)) + "' > filename");
                executeLzyCommand(
                    ServantCommandHolder.publish,
                    "filename",
                    "-z",
                    serverAddress() // serverAddress
                );
                TimeUtils.waitFlagUp(
                    () -> pathExists(Path.of(mount() + "/bin/" + zygote.name())),
                    30,
                    TimeUnit.SECONDS
                );
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }

        default String parseChannelIdFromCreateChannelResponse(String response) {
            var builder = ChannelManager.ChannelCreateResponse.newBuilder();
            try {
                JsonFormat.parser().merge(response, builder);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            return builder.build().getChannelId();
        }

        default String createChannel(String channelName) {
            final String response = executeLzyCommand(
                BuiltinCommandHolder.channel,
                "create",
                "-n", channelName,
                "-t", "direct"
            ).stdout();
            return parseChannelIdFromCreateChannelResponse(response);
        }

        default String createChannel(String channelName, String snapshotId, String entryId) {
            final String response = executeLzyCommand(
                BuiltinCommandHolder.channel,
                "create",
                "-n", channelName,
                "-t", "snapshot",
                "-s", snapshotId,
                "-e", entryId
            ).stdout();
            return parseChannelIdFromCreateChannelResponse(response);
        }

        default String channelsStatus() {
            final ExecutionResult execute = executeLzyCommand(BuiltinCommandHolder.cs);
            return execute.stdout();
        }

        default String getWhiteboard(String wbId) {
            final ExecutionResult execute = executeLzyCommand(
                ServantCommandHolder.whiteboard,
                "get",
                wbId
            );
            return execute.stdout();
        }

        default String getAllWhiteboards() {
            final ExecutionResult execute = executeLzyCommand(
                ServantCommandHolder.whiteboard,
                "getAll"
            );
            return execute.stdout();
        }

        default String whiteboards(String namespace, List<String> tags,
                                   Long fromDateLocalTimezone, Long toDateLocalTimezone) {
            String command = String.join(
                " ",
                "list",
                "-n",
                namespace,
                "-from",
                fromDateLocalTimezone.toString(),
                "-to",
                toDateLocalTimezone.toString()
            );
            if (!tags.isEmpty()) {
                command = String.join(" ", command, "-t", String.join(",", tags));
            }
            final ExecutionResult execute = executeLzyCommand(ServantCommandHolder.whiteboard, command);
            return execute.stdout();
        }

        default String createSnapshot(String workflowName) {
            final ExecutionResult execute = executeLzyCommand(
                ServantCommandHolder.snapshot,
                "create",
                workflowName
            );
            return execute.stdout();
        }

        default String createWhiteboard(String wbId, List<String> fieldNames, List<String> tags, String namespace) {
            String command = String.join(
                " ",
                "create",
                wbId,
                "-l",
                String.join(",", fieldNames),
                "-n",
                namespace
            );
            if (!tags.isEmpty()) {
                command = String.join(" ", command, "-t", String.join(",", tags));
            }
            final ExecutionResult execute = executeLzyCommand(ServantCommandHolder.whiteboard, command);
            return execute.stdout();
        }

        default void link(String wbId, String fieldId, String entryId) {
            executeLzyCommand(
                ServantCommandHolder.whiteboard,
                "link",
                wbId,
                "-f",
                fieldId,
                "-e",
                entryId
            );
        }

        default void finalizeSnapshot(String spId) {
            final ExecutionResult execute = executeLzyCommand(
                ServantCommandHolder.snapshot,
                "finalize",
                spId
            );
        }

        default void destroyChannel(String channelName) {
            executeLzyCommand(
                BuiltinCommandHolder.channel,
                "destroy",
                channelName
            );
        }

        default String channelStatus(String channelName) {
            final ExecutionResult execute = executeLzyCommand(
                BuiltinCommandHolder.channel,
                "status",
                channelName
            );
            return execute.stdout();
        }

        default String sessions() {
            final ExecutionResult execute = executeLzyCommand(ServantCommandHolder.sessions);
            return execute.stdout();
        }

        default void createSlot(String path, String channelId, Slot slot) {
            try {
                execute("echo '" + JsonFormat.printer().print(ProtoConverter.toProto(slot)) + "' > slot.json");
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            executeLzyCommand(
                BuiltinCommandHolder.touch,
                path,
                channelId,
                "--slot",
                "slot.json"
            );
            if (slot.direction() == Slot.Direction.OUTPUT) {
                TimeUtils.waitFlagUp(() -> pathExists(Path.of(path)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
            }
        }

        default void update() {
            executeLzyCommand(ServantCommandHolder.update);
        }

        default boolean waitForStatus(AgentStatus status, long timeout, TimeUnit unit) {
            return TimeUtils.waitFlagUp(() -> Objects.equals(status(), status), timeout, unit);
        }

        @Nullable
        AgentStatus status();

        boolean waitForShutdown();

        void shutdownNow();

        interface ExecutionResult {

            String stdout();

            String stderr();

            int exitCode();
        }
    }
}
