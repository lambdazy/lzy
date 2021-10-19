package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServantDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;

public class LzyBaseTest {
    protected static final int DEFAULT_TIMEOUT_SEC = 30;
    protected static final int DEFAULT_SERVANT_PORT = 9999;
    protected static final String LZY_MOUNT = "/tmp/lzy";

    protected LzyServantTestContext servantContext;
    protected LzyServerTestContext serverContext;

    @Before
    public void setUp() {
        servantContext = new LzyServantDockerContext();
        serverContext = new LzyServerProcessesContext();
    }

    @After
    public void tearDown() {
        servantContext.close();
        serverContext.close();
    }
}
