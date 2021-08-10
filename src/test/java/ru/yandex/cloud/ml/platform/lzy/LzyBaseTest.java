package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LzyBaseTest {
    private static final int LZY_SERVER_PORT = 7777;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

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
        executorService.shutdown();
    }

    protected void startTerminalAtPath(String path) {
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
        executorService.submit(() -> {
            try {
                LzyServant.main(lzyArgs);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected boolean waitForFuse(String path, long timeout, TimeUnit unit) throws InterruptedException {
        final long finish = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
        while (System.currentTimeMillis() < finish) {
            final Path lzyPath = Path.of(path);
            if (Files.isDirectory(lzyPath)) {
                final File lzyDir = new File(String.valueOf(lzyPath));
                if (lzyDir.list().length > 0) {
                    return true;
                }
            }
            //noinspection BusyWait
            Thread.sleep(1000);
        }
        return false;
    }
}
