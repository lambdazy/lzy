package ru.yandex.cloud.ml.platform.lzy.test;

import io.findify.s3mock.S3Mock;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyKharonThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzySnapshotThreadContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

/*
 * Start Lzy Env with following settings:
 *   - server port:        7777
 *   - snapshot port:      8999
 *   - whiteboard port:    8999
 *   - s3 port:            8001
 *   - kharon port (opt):  8899
 *
 * Client terminal:
 *
 *     $ lzy-terminal -s http://localhost:7777 -m /tmp/lzy -u user
 *
 */
public class LocalRunner {

    private static void createFolder(Path path) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        Configurator.setRootLevel(Level.INFO);

        createFolder(Path.of("/tmp/resources/"));
        createFolder(Path.of("/tmp/servant/lzy/"));

        var serverContext = new LzyServerThreadContext();
        serverContext.init();

        var whiteboardContext = new LzySnapshotThreadContext(serverContext.address());
        whiteboardContext.init();

        final LzyKharonThreadContext kharonContext;
        if (args.length > 1 && args[1].equals("--no-kharon")) {
            System.out.println("Run without Kharon...");
            kharonContext = null;
        } else {
            kharonContext = new LzyKharonThreadContext(serverContext.address(), whiteboardContext.address());
            kharonContext.init();
        }

        var s3 = new S3Mock.Builder()
            .withPort(8001)
            .withInMemoryBackend()
            .build();
        s3.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Terminate...");
            s3.shutdown();
            if (kharonContext != null) {
                kharonContext.close();
            }
            serverContext.close();
            whiteboardContext.close();
        }));

        LockSupport.park();
    }
}
