package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import ru.yandex.cloud.ml.platform.lzy.test.*;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyKharonProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyTerminalDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzySnapshotProcessesContext;

public class LzyBaseDockerTest implements LzyTest {
    private static final int DEFAULT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9999;
    private static final String DEFAULT_LZY_MOUNT = "/tmp/lzy";

    private LzyTerminalTestContext terminalContext;
    private LzyServerTestContext ser;
    private LzyKharonTestContext kharonCo;
    private LzySnapshotTestContext whiteboardContext;

    @Before
    public void setUp() {
        ser = new LzyServerProcessesContext(TasksConfig.TaskType.LOCAL_DOCKER);
        ser.init();
        whiteboardContext = new LzySnapshotProcessesContext();
        whiteboardContext.init();
        kharonCo = new LzyKharonProcessesContext(ser.address(false), whiteboardContext.address(false));
        kharonCo.init(true);
        terminalContext = new LzyTerminalDockerContext();
    }

    @After
    public void tearDown() {
        terminalContext.close();
        kharonCo.close();
        ser.close();
        whiteboardContext.close();
    }

    @Override
    public LzyTerminalTestContext terminalContext() {
        return terminalContext;
    }

    @Override
    public LzyServerTestContext serverContext() {
        return ser;
    }

    @Override
    public LzyKharonTestContext kharonContext() {
        return kharonCo;
    }

    @Override
    public LzySnapshotTestContext whiteboardContext() {
        return whiteboardContext;
    }

    @Override
    public String defaultLzyMount() {
        return DEFAULT_LZY_MOUNT;
    }

    @Override
    public int defaultServantPort() {
        return DEFAULT_SERVANT_PORT;
    }

    @Override
    public int defaultTimeoutSec() {
        return DEFAULT_TIMEOUT_SEC;
    }
}
