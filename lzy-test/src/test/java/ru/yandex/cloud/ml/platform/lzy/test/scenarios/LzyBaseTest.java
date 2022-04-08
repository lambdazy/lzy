package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.*;

public class LzyBaseTest {
    protected static final int DEFAULT_TIMEOUT_SEC = 30;
    protected static final int DEFAULT_SERVANT_PORT = 9999;
    protected static final String LZY_MOUNT = "/tmp/lzy";
    protected static final int S3_PORT = 8001;

    protected LzyTerminalTestContext terminalContext;
    protected LzyPythonTerminalDockerContext pyTerminalContext;
    protected LzyServerTestContext serverContext;
    protected LzyKharonTestContext kharonContext;
    protected LzySnapshotTestContext whiteboardContext;
    protected S3Mock api;

    @Before
    public void setUp() {
        createResourcesFolder();
        createServantLzyFolder();
        serverContext = new LzyServerThreadContext();
        serverContext.init();
        whiteboardContext = new LzySnapshotThreadContext(serverContext.address(false));
        whiteboardContext.init();
        kharonContext = new LzyKharonThreadContext(serverContext.address(false), whiteboardContext.address(false));
        kharonContext.init();
        terminalContext = new TerminalThreadContext();
        pyTerminalContext = new LzyPythonTerminalDockerContext();
        api = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
        api.start();
    }

    @After
    public void tearDown() {
        api.shutdown();
        terminalContext.close();
        pyTerminalContext.close();
        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
    }

    private static void createResourcesFolder() {
        createFolder(Path.of("/tmp/resources/"));
    }

    private static void createServantLzyFolder() {
        createFolder(Path.of("/tmp/servant/lzy/"));
    }

    private static void createFolder(Path path) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
