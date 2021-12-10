package ru.yandex.cloud.ml.platform.lzy.test.scenarios.docker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import ru.yandex.cloud.ml.platform.lzy.test.scenarios.LzyBaseDockerTest;

public class PyApiTest extends LzyBaseDockerTest {
    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext().startTerminalAtPathAndPort(
                defaultLzyMount(),
                9999,
                kharonContext().serverAddress(terminalContext().inDocker())
        );
        terminal.waitForStatus(
                AgentStatus.EXECUTING,
                defaultTimeoutSec(),
                TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimplePyGraph() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
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
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
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

    @Test
    public void testExecFail() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/examples/test_tasks/exec_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stderr().contains("LzyExecutionException"));
    }

    @Test
    public void testEnvFail() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/examples/test_tasks/env_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stderr().contains("Failed to install environment on remote machine"));
    }

    @Test
    public void testSimpleWhiteboard() {
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/examples/integration/whiteboard_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("42 42"));
    }
}
