package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PyRunTest extends LzyBaseTest {
    private LzyServantTestContext.Servant terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                9999,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );
        terminal.waitForStatus(
                ServantStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimplePyGraph() {
        //Arrange
        final String pyCommand = "python3 /lzy-python/examples/integration/simple_graph.py";

        //Act
        final LzyServantTestContext.Servant.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c", pyCommand);

        //Assert
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result.stdout()));
    }
}
