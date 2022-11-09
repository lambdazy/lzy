package ai.lzy.test.scenarios;

import ai.lzy.fs.LzyFsServerLegacy;
import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.test.*;
import ai.lzy.test.impl.*;
import ai.lzy.util.auth.credentials.RsaUtils;
import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public abstract class LocalScenario extends LzyBaseTest {

    static class Config extends Utils.Defaults {

        protected static final int S3_PORT = 8001;
    }

    protected LzyIAMTestContext iamContext;
    protected LzyStorageTestContext storageContext;
    protected LzyServerTestContext serverContext;
    protected LzySnapshotTestContext whiteboardContext;
    protected LzyKharonTestContext kharonContext;
    protected LzyChannelManagerContext channelManagerContext;
    protected S3Mock s3Mock;
    protected LzyTerminalTestContext.Terminal terminal;
    protected RsaUtils.RsaKeysFiles terminalKeys;

    @Before
    public void setUp() {
        this.setUp(LzyServerTestContext.LocalServantAllocatorType.THREAD_ALLOCATOR, true);
    }

    public void setUp(LzyServerTestContext.LocalServantAllocatorType servantAllocatorType,
                      boolean stubIamForChannelManager)
    {
        createResourcesFolder();
        createServantLzyFolder();

        try {
            terminalKeys = RsaUtils.generateRsaKeysFiles();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        iamContext = new IAMThreadContext();
        iamContext.init();

        serverContext = new ServerThreadContext(servantAllocatorType);
        serverContext.init();

        whiteboardContext = new SnapshotThreadContext(serverContext.address());
        whiteboardContext.init();

        channelManagerContext = new ChannelManagerThreadContext(whiteboardContext.address(), iamContext.address());
        channelManagerContext.init(stubIamForChannelManager);

        kharonContext = new KharonThreadContext(
            serverContext.address(),
            whiteboardContext.address(),
            channelManagerContext.address()
        );
        kharonContext.init();

        s3Mock = new S3Mock.Builder().withPort(Config.S3_PORT).withInMemoryBackend().build();
        s3Mock.start();

        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
        //wait until all servants unmount fs
        final boolean flagUp = TimeUtils.waitFlagUp(() -> LzyFsServerLegacy.mounted.get() == 0, 60, TimeUnit.SECONDS);
        Assert.assertTrue("Not all fs servers are unmounted", flagUp);

        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
        channelManagerContext.close();
        iamContext.close();
        s3Mock.shutdown();
    }

    public void startTerminalWithDefaultConfig() {
        terminal = terminalContext.startTerminalAtPathAndPort(
            Config.LZY_MOUNT,
            Config.SERVANT_PORT,
            Config.SERVANT_FS_PORT,
            kharonContext.serverAddress(),
            kharonContext.channelManagerProxyAddress(),
            Config.DEBUG_PORT,
            terminalContext.TEST_USER,
            terminalKeys.privateKeyPath().toString());
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
