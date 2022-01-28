package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyTerminalDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KuberRunTest {
    private static final int DEFAULT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9999;
    private static final String LZY_MOUNT = "/tmp/lzy";
    private static final String TEST_USER = "phil";
    private static final String TEST_USER_KEY_PATH = "/tmp/test-private.pem";
    private static final String LZY_KHARON_DOMAIN_PROPERTY = "lzy.kharon.domain";
    private static final String DEFAULT_LZY_KHARON_DOMAIN = "kharon-lzy-prod.northeurope.cloudapp.azure.com";
    private final String LZY_KHARON_DOMAIN = System.getProperty(LZY_KHARON_DOMAIN_PROPERTY, DEFAULT_LZY_KHARON_DOMAIN);
    private final String SERVER_URL = String.format("http://%s:8899", LZY_KHARON_DOMAIN);
    private final LzyTerminalTestContext terminalContext = new LzyTerminalDockerContext();
    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            DEFAULT_SERVANT_PORT,
            SERVER_URL,
            5006,
            TEST_USER,
            TEST_USER_KEY_PATH
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    @After
    public void tearDown() {
        terminalContext.close();
    }

    @Test
    public void testSimplePyGraph() {
        //Arrange
        final String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/simple_graph.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result.stdout()));
        Assert.assertTrue(result.stdout().contains("Just print some text"));
    }

    @Test
    public void testSimpleCatboostGraph() {
        //Arrange
        String condaPrefix = prepareConda();
        terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install catboost");
        final String pyCommand = "python /lzy-python/tests/scenarios/catboost_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("1", Utils.lastLine(result.stdout()));
    }

    @Test
    public void testSimpleWhiteboard() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/whiteboard_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("42 42"));
    }

    private String prepareConda() {
        String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
                "conda activate default && ";
        terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
        return condaPrefix;
    }
}
