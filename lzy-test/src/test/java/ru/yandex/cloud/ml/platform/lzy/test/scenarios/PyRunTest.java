package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
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
                kharonContext.serverAddress(servantContext.inDocker())
        );
        terminal.waitForStatus(
                AgentStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimplePyGraph() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

        //Act
        final LzyServantTestContext.Servant.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result.stdout()));
    }

    @Test
    public void testSimpleCatboostGraph() {
        //Arrange
        String condaPrefix = prepareConda();
        terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install catboost");
        final String pyCommand = "python /lzy-python/examples/integration/catboost_simple.py";

        //Act
        final LzyServantTestContext.Servant.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("1", Utils.lastLine(result.stdout()));
    }

    private String prepareConda() {
        String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
                "conda activate default && ";
        terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
        return condaPrefix;
    }
}
