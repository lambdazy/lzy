package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LzyBaseTest {
    private static final int LZY_SERVER_PORT = 7777;
    private final List<Process> servantProcesses = new ArrayList<>();

    private Server lzyServer;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;


    @Before
    public void setUp() throws Exception {
        lzyServer = ServerBuilder.forPort(LZY_SERVER_PORT).addService(new LzyServer.Impl()).build();
        lzyServer.start();

        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", LZY_SERVER_PORT)
            .usePlaintext()
            .build();
        lzyServerClient = LzyServerGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws InterruptedException {
        lzyServer.shutdown();
        lzyServer.awaitTermination();
        servantProcesses.forEach(Process::destroy);
    }

    protected void startTerminalAtPath(String path) throws IOException {
        final String[] lzyArgs = {
            "terminal",
            "--lzy-address",
            "localhost:" + LZY_SERVER_PORT,
            "--host",
            "localhost",
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
        final Process process = builder.inheritIO().start();
        servantProcesses.add(process);
    }

    protected boolean waitForServants(long timeout, TimeUnit unit, int... ports) throws InterruptedException {
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
                //noinspection BusyWait
                Thread.sleep(1000);
            }
        }
        return false;
    }
}
