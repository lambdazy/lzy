package ai.lzy.test.scenarios;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.test.LzyKharonTestContext;
import ai.lzy.test.LzyServerTestContext;
import ai.lzy.test.LzySnapshotTestContext;
import ai.lzy.test.LzyTerminalTestContext;
import ai.lzy.test.impl.KharonThreadContext;
import ai.lzy.test.impl.ServerThreadContext;
import ai.lzy.test.impl.SnapshotThreadContext;
import ai.lzy.test.impl.Utils;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import ai.lzy.servant.agents.AgentStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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
        this.setUp(LzyServerTestContext.LocalServantAllocatorType.THREAD_ALLOCATOR);
    }

    public void setUp(LzyServerTestContext.LocalServantAllocatorType servantAllocatorType) {
        createResourcesFolder();
        createServantLzyFolder();
        serverContext = new ServerThreadContext(servantAllocatorType);
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
    public void tearDown() throws InterruptedException {
        super.tearDown();
        //wait until all servants unmount fs
        while (LzyFsServer.mounted.get() != 0) {
            //noinspection BusyWait
            Thread.sleep(500);
        }

        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
        s3Mock.shutdown();
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
