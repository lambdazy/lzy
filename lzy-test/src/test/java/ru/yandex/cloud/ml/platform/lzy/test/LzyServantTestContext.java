package ru.yandex.cloud.ml.platform.lzy.test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface LzyServantTestContext extends AutoCloseable {
    Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort);

    boolean inDocker();

    void close();

    interface Servant {
        boolean pathExists(Path path);

        String mount();

        int port();

        String serverHost();

        ExecutionResult execute(Map<String, String> env, String... command);

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

        boolean waitForStatus(ServantStatus status, long timeout, TimeUnit unit);

        boolean waitForShutdown(long timeout, TimeUnit unit);

        interface ExecutionResult {
            String stdout();

            String stderr();

            int exitCode();
        }
    }
}
