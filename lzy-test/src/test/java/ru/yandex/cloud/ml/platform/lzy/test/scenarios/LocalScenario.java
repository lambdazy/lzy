package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyKharonThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzySnapshotThreadContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public abstract class LocalScenario extends LzyBaseTest {
    static class Config extends Defaults {
        protected static final int S3_PORT = 8001;
    }

    protected LzyServerTestContext serverContext;
    protected LzySnapshotTestContext whiteboardContext;
    protected LzyKharonTestContext kharonContext;
    protected S3Mock api;

    protected boolean status = false;
    protected LzyTerminalTestContext.Terminal terminal;
    @Before
    public void setUp() {
        createResourcesFolder();
        createServantLzyFolder();
        serverContext = new LzyServerThreadContext();
        serverContext.init();
        whiteboardContext = new LzySnapshotThreadContext(serverContext.address());
        whiteboardContext.init();
        kharonContext = new LzyKharonThreadContext(serverContext.address(), whiteboardContext.address());
        kharonContext.init();
        api = new S3Mock.Builder().withPort(Config.S3_PORT).withInMemoryBackend().build();
        api.start();
        super.setUp();
    }

    @After
    public void tearDown() {
        api.shutdown();
        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
        super.tearDown();
    }

    public void startTerminalWithDefaultConfig() {
        terminal = terminalContext.startTerminalAtPathAndPort(
                Config.LZY_MOUNT,
                Config.SERVANT_PORT,
                Config.SERVANT_FS_PORT,
                kharonContext.serverAddress(),
                Config.DEBUG_PORT,
                terminalContext.TEST_USER,
                null
        );
        status = terminal.waitForStatus(
                AgentStatus.EXECUTING,
                Config.TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }

    public void stopTerminal() {
        // terminal = null; ??
        status = false;
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
