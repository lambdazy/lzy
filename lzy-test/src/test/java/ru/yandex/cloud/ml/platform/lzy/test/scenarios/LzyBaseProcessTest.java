package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.findify.s3mock.S3Mock;
import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyKharonProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzySnapshotProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyTerminalProcessesContext;

public class LzyBaseProcessTest {
    protected static final int DEFAULT_TIMEOUT_SEC = 30;
    protected static final int DEFAULT_SERVANT_PORT = 9999;
    protected static final String LZY_MOUNT = "/tmp/lzy";
    protected static final int S3_PORT = 8001;

    protected LzyTerminalTestContext terminalContext;
    protected LzyServerTestContext serverContext;
    protected LzyKharonTestContext kharonContext;
    protected LzySnapshotTestContext whiteboardContext;
    protected S3Mock api;

    @Before
    public void setUp() {
        serverContext = new LzyServerProcessesContext(TasksConfig.TaskType.LOCAL_PROCESS);
        serverContext.init();
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
        api.shutdown();
        terminalContext.close();
        kharonContext.close();
        serverContext.close();
        whiteboardContext.close();
    }
}
