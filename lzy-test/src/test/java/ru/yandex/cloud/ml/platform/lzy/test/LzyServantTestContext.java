package ru.yandex.cloud.ml.platform.lzy.test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface LzyServantTestContext extends AutoCloseable {
    int DEFAULT_TIMEOUT_SEC = 30;

    Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort);

    boolean inDocker();
    void close();

    interface Servant {
        boolean pathExists(Path path);
        String mount();
        @SuppressWarnings("unused")
        int port();
        String serverHost();

        @SuppressWarnings("UnusedReturnValue")
        default ExecutionResult execute(String... command) {
            return execute(Collections.emptyMap(), command);
        }

        ExecutionResult execute(Map<String, String> env, String... command);

        default ExecutionResult run(String zygoteName, String arguments) {
            return execute(
                Collections.emptyMap(),
                "/bin/bash",
                "-c",
                String.join(
                    " ",
                    mount() + "/bin/" + zygoteName,
                    arguments
                )
            );
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
                        serverHost() // serverAddress
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
            Utils.waitFlagUp(() -> pathExists(Path.of(path)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        boolean waitForStatus(ServantStatus status, long timeout, TimeUnit unit);
        boolean waitForShutdown(long timeout, TimeUnit unit);

        interface ExecutionResult {
            String stdout();
            String stderr();
            int exitCode();
        }
    }
}
