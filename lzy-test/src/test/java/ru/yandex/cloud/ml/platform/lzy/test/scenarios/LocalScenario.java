package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.KharonThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.ServerThreadContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.SnapshotThreadContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

public abstract class LocalScenario extends LzyBaseTest {
    static class Config extends Utils.Defaults {
        protected static final int S3_PORT = 8001;
    }

    protected LzyServerTestContext serverContext;
    protected LzySnapshotTestContext whiteboardContext;
    protected LzyKharonTestContext kharonContext;
    protected S3Mock s3Mock;
    protected LzyTerminalTestContext.Terminal terminal;
    @Before
    public void setUp() {
        createResourcesFolder();
        createServantLzyFolder();
        serverContext = new ServerThreadContext();
        serverContext.init();
        whiteboardContext = new SnapshotThreadContext(serverContext.address());
        whiteboardContext.init();
        kharonContext = new KharonThreadContext(serverContext.address(), whiteboardContext.address());
        kharonContext.init();
        s3Mock = new S3Mock.Builder().withPort(Config.S3_PORT).withInMemoryBackend().build();
        s3Mock.start();
        super.setUp();
    }

    @After
    public void tearDown() {
        s3Mock.shutdown();
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
        Assert.assertTrue(terminal.waitForStatus(
            AgentStatus.EXECUTING,
            Config.TIMEOUT_SEC,
            TimeUnit.SECONDS
        ));
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
