package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LzyLocalProcessesTestContext implements LzyTestContext {
    private static final int DEFAULT_SERVANT_TIMEOUT_SEC = 30;
    private static final int LZY_SERVER_PORT = 7777;
    private final List<Process> servantProcesses = new ArrayList<>();

    private Server lzyServer;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;

    public void start() {
        lzyServer = ServerBuilder.forPort(LZY_SERVER_PORT).addService(new LzyServer.Impl()).build();
        try {
            lzyServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", LZY_SERVER_PORT)
            .usePlaintext()
            .build();
        lzyServerClient = LzyServerGrpc.newBlockingStub(channel);
    }

    @After
    public void stop() {
        lzyServer.shutdown();
        try {
            lzyServer.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            servantProcesses.forEach(Process::destroy);
        }
    }

    @Override
    public LzyServerGrpc.LzyServerBlockingStub server() {
        return lzyServerClient;
    }

    public boolean startTerminalAtPathAndPort(String path, int port) {
        final String[] lzyArgs = {
            "terminal",
            "--lzy-address",
            "localhost:" + LZY_SERVER_PORT,
            "--host",
            "localhost",
            "--port",
            String.valueOf(port),
            "--lzy-mount",
            path,
            "--private-key",
            "/tmp/nonexistent-key"
        };
        final List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin" + "/java");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LzyServant.class.getCanonicalName());
        command.addAll(Arrays.asList(lzyArgs));

        final ProcessBuilder builder = new ProcessBuilder(command);
        final Process process;
        try {
            process = builder.inheritIO().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        servantProcesses.add(process);
        return waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, port);
    }

    public boolean waitForServants(long timeout, TimeUnit unit, int... ports) {
        final long finish = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
        final Set<Integer> expected = Arrays.stream(ports).boxed().collect(Collectors.toSet());
        while (System.currentTimeMillis() < finish) {
            final Set<Integer> actual = lzyServerClient.servantsStatus(IAM.Auth.newBuilder()
                .setUser(IAM.UserCredentials.newBuilder()
                    .setUserId("uid")
                    .setToken("token")
                    .build()
                ).build()).getStatusesList()
                .stream()
                .map(value -> URI.create(value.getServantURI()).getPort())
                .collect(Collectors.toSet());
            if (expected.equals(actual)) {
                return true;
            } else {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }

    @Override
    public boolean pathExists(Path path) {
        return Files.exists(path);
    }
}
