package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

public class PyApiTest extends LzyBaseTest {
    private LzyTerminalTestContext.Terminal terminal;

    public void arrangeTerminal(String user) {
        this.arrangeTerminal(LZY_MOUNT, 9998, kharonContext.serverAddress(terminalContext.inDocker()), user, null);
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
        arrangeTerminal("testUser");
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
        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python /lzy-python/tests/scenarios/simple_graph_with_assertions.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertEquals("6", Utils.lastLine(result.stdout()));
    }
    @Test
    public void testSimpleCatboostGraph() {
        //Arrange
        arrangeTerminal("testUser");
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
    public void testExecFail() {
        //Arrange
        arrangeTerminal("phil");
        final String pyCommand = "python /lzy-python/tests/scenarios/exec_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stderr().contains("LzyExecutionException"));
    }

    @Test
    public void testEnvFail() {
        //Arrange
        arrangeTerminal("phil");
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
        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python /lzy-python/tests/scenarios/whiteboard_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("42 42"));
        Assert.assertTrue(result.stdout().contains("COMPLETED"));
    }

    @Test
    public void testSimpleView() {
        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python /lzy-python/tests/scenarios/view_simple.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(), "bash", "-c",
                condaPrefix + pyCommand);

        // Assert
        Assert.assertTrue(result.stdout().contains("Number of SimpleView views 6"));
        Assert.assertTrue(result.stdout().contains("Ids of SimpleView second_id_SimpleWhiteboard;" +
                "first_id_SimpleWhiteboard;second_id_SimpleWhiteboard;first_id_SimpleWhiteboard;" +
                "third_id_OneMoreSimpleWhiteboard;third_id_OneMoreSimpleWhiteboard;"));
        Assert.assertTrue(result.stdout().contains("Rules of SimpleView minus_one_rule;plus_one_rule;minus_one_rule;" +
                "plus_one_rule;plus_two_rule;plus_two_rule;"));

        Assert.assertTrue(result.stdout().contains("Number of AnotherSimpleView views 3"));
        Assert.assertTrue(result.stdout().contains("Ids of AnotherSimpleView first_id_SimpleWhiteboard;" +
                "first_id_SimpleWhiteboard;3;"));

        Assert.assertTrue(result.stdout().contains("Iterating over whiteboards with types SimpleWhiteboard " +
                "SimpleWhiteboard AnotherSimpleWhiteboard"));
        Assert.assertTrue(result.stdout().contains("Number of whiteboard is 3"));
        Assert.assertTrue(result.stdout().contains("First whiteboard type is SimpleWhiteboard"));
    }
}
