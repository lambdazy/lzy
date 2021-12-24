package ru.yandex.cloud.ml.platform.lzy.test.scenarios.processes;

import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTest;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyKharonProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzySnapshotProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyTerminalProcessesContext;

public class LzyBaseProcessTest implements LzyTest {
    private static final int S3_PORT = 8001;
    private static final int DEFAULT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9999;
    private static final String LZY_MOUNT = "/tmp/lzy";

    private LzyTerminalTestContext terminalContext;
    private LzyServerTestContext serverContext;
    private LzyKharonTestContext kharonContext;
    private LzySnapshotTestContext whiteboardContext;
    private S3Mock api;

    @Before
    public void setUp() {
        serverContext = new LzyServerProcessesContext(TasksConfig.TaskType.LOCAL_PROCESS);
        serverContext.init(false);
        whiteboardContext = new LzySnapshotProcessesContext(serverContext.address(false));
        whiteboardContext.init();
        kharonContext = new LzyKharonProcessesContext(serverContext.address(false), whiteboardContext.address(false));
        kharonContext.init(false);
        terminalContext = new LzyTerminalProcessesContext();
        api = new S3Mock.Builder().withPort(S3_PORT).withInMemoryBackend().build();
        api.start();
    }

    @After
    public void tearDown() {
        terminalContext.close();
        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
        api.shutdown();
    }

    @Override
    public LzyTerminalTestContext terminalContext() {
        return terminalContext;
    }

    @Override
    public LzyServerTestContext serverContext() {
        return serverContext;
    }

    @Override
    public LzyKharonTestContext kharonContext() {
        return kharonContext;
    }

    @Override
    public LzySnapshotTestContext whiteboardContext() {
        return whiteboardContext;
    }

    @Override
    public String defaultLzyMount() {
        return LZY_MOUNT;
    }

    @Override
    public int defaultServantPort() {
        return DEFAULT_SERVANT_PORT;
    }

    @Override
    public int defaultTimeoutSec() {
        return DEFAULT_TIMEOUT_SEC;
    }

    @Override
    public int s3Port() {
        return S3_PORT;
    }
}
