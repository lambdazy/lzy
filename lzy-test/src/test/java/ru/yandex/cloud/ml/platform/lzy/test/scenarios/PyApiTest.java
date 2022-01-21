package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PyApiTest extends LzyBaseTest {
    private LzyTerminalTestContext.Terminal terminal;

    public void arrangeTerminal() {
        terminal = pyTerminalContext.startTerminalAtPathAndPort(
                LZY_MOUNT, 9999, kharonContext.serverAddress(terminalContext.inDocker())
        );
        terminal.waitForStatus(
                AgentStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }
    public void arrangeTerminal(String user) {
        this.arrangeTerminal(LZY_MOUNT, 9999, kharonContext.serverAddress(terminalContext.inDocker()), user, null);
    }

    public void arrangeTerminal(String serverAddress, int port, String user) {
        this.arrangeTerminal(LZY_MOUNT, port, serverAddress, user, null);
    }

    public void arrangeTerminal(String mount, Integer port, String serverAddress, String user, String keyPath) {
        int debugPort = 5006;
        terminal = pyTerminalContext.startTerminalAtPathAndPort(mount, port, serverAddress, debugPort, user, keyPath );
        terminal.waitForStatus(
                AgentStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimplePyGraph() {
        //Arrange
        arrangeTerminal(
                "localhost",
                8899,
                "test_user"
        );
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
    public void testSimplePyGraphWithAssertions() {
        arrangeTerminal(
                "localhost",
                8899,
                "test_user"
        );
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/simple_graph_with_assertions.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("6", Utils.lastLine(result.stdout()));
    }
    @Test
    public void testSimpleCatboostGraph() {
        arrangeTerminal();
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

    private String prepareConda() {
        String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
                "conda activate default && ";
        terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
        return condaPrefix;
    }

    @Test
    public void testExecFail() {
        arrangeTerminal("phil");
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/exec_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stderr().contains("LzyExecutionException"));
    }

    @Test
    public void testEnvFail() {
        arrangeTerminal("phil");
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/env_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stderr().contains("Could not find a version that satisfies the requirement"));
        Assert.assertTrue(result.stderr().contains("Failed to install environment on remote machine"));
    }

    @Test
    public void testSimpleWhiteboard() {
        arrangeTerminal(
                "localhost",
                8899,
                "test_user"
                );
        //Arrange
        String condaPrefix = prepareConda();
        final String pyCommand = "python /lzy-python/tests/scenarios/whiteboard_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("42 42"));
        Assert.assertTrue(result.stdout().contains("COMPLETED"));
    }
}
