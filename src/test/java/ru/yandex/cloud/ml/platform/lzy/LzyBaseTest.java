package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LzyBaseTest {
    private static final int LZY_SERVER_PORT = 7777;
    private final List<Process> servantProcesses = new ArrayList<>();
    protected Server lzyServer;

    @Before
    public void setUp() throws Exception {
        lzyServer = ServerBuilder.forPort(LZY_SERVER_PORT).addService(new LzyServer.Impl()).build();
        lzyServer.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        lzyServer.shutdown();
        lzyServer.awaitTermination();
        servantProcesses.forEach(Process::destroy);
    }

    protected void startTerminalAtPath(String path) throws Exception {
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
        command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LzyServant.class.getCanonicalName());
        command.addAll(Arrays.asList(lzyArgs));

        final ProcessBuilder builder = new ProcessBuilder(command);
        final Process process = builder.inheritIO().start();
        servantProcesses.add(process);
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
