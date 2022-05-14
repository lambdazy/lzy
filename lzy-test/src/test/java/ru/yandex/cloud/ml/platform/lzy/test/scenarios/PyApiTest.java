package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

public class PyApiTest extends LzyBaseTest {
    private static final Logger LOG = LogManager.getLogger(PyApiTest.class);

    private LzyTerminalTestContext.Terminal terminal;

    public void arrangeTerminal(String user) {
        this.arrangeTerminal(LZY_MOUNT, FreePortFinder.find(20000, 21000), FreePortFinder.find(21000, 22000),
            kharonContext.serverAddress(), user, null);
    }

    public void arrangeTerminal(String mount, int port, int fsPort, String serverAddress, String user,
                                String keyPath) {
        int debugPort = FreePortFinder.find(22000, 23000);
        terminal = terminalContext.startTerminalAtPathAndPort(mount, port, fsPort, serverAddress,
            debugPort, user, keyPath);
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */

        //Arrange
        arrangeTerminal("testUser");
        terminal.execute(Map.of(), "bash", "-c",
            condaPrefix + "pip install catboost");
        final String pyCommand = "python ../lzy-python/tests/scenarios/catboost_integration_cpu.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        LOG.info("testSimpleCatboostGraph: STDOUT: {}", result.stdout());
        LOG.info("testSimpleCatboostGraph: STDERR: {}", result.stderr());

        //Assert
        Assert.assertTrue(result.stdout().contains("Prediction: 1"));
    }

    @Test
    public void testExecFail() {
        //Arrange
        arrangeTerminal("phil");
        final String pyCommand = "python ../lzy-python/tests/scenarios/exec_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        LOG.info("testExecFail: STDOUT: {}", result.stdout());
        LOG.info("testExecFail: STDERR: {}", result.stderr());

        //Assert
        Assert.assertTrue(result.stderr().contains("LzyExecutionException"));
    }

    @Test
    public void testEnvFail() {
        //Arrange
        arrangeTerminal("phil");
        final String pyCommand = "python ../lzy-python/tests/scenarios/env_fail.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        LOG.info("testEnvFail: STDOUT: {}", result.stdout());
        LOG.info("testEnvFail: STDERR: {}", result.stderr());

        //Assert
        Assert.assertTrue(
            result.stderr().contains("Could not find a version that satisfies the requirement"));
        Assert.assertTrue(
            result.stderr().contains("Failed to install environment on remote machine"));
    }

    @Test
    public void testCache() {
        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python ../lzy-python/tests/scenarios/test_cache.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        LOG.info("testCache: STDOUT: {}", result.stdout());
        LOG.info("testCache: STDERR: {}", result.stderr());
        Assert.assertTrue(result.stdout().contains("Is fun2 cached? True"));
    }
    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */

        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python ../lzy-python/tests/scenarios/import.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        LOG.info("testImportFile: STDOUT: {}", result.stdout());
        LOG.info("testImportFile: STDERR: {}", result.stderr());
        Assert.assertTrue(result.stdout().contains("bar base echo"));
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python ../lzy-python/tests/scenarios/none_result.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("None"));
    }

    @Test
    public void testUberGraph() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */

        //Arrange
        arrangeTerminal("testUser");
        final String pyCommand = "python ../lzy-python/tests/scenarios/uber/graph.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        LOG.info("testUberGraph: STDOUT: {}", result.stdout());
        LOG.info("testUberGraph: STDERR: {}", result.stderr());

        // Assert
        Assert.assertTrue(result.stdout().contains("42 42"));
        Assert.assertTrue(result.stdout().contains("Len: 3"));

        Assert.assertTrue(result.stdout().contains("Number of SimpleView views 6"));
        Assert.assertTrue(result.stdout().contains("Ids of SimpleView second_id_SimpleWhiteboard;" +
            "first_id_SimpleWhiteboard;second_id_SimpleWhiteboard;first_id_SimpleWhiteboard;" +
            "third_id_OneMoreSimpleWhiteboard;third_id_OneMoreSimpleWhiteboard;"));
        Assert.assertTrue(result.stdout()
            .contains("Rules of SimpleView minus_one_rule;plus_one_rule;minus_one_rule;" +
                "plus_one_rule;plus_two_rule;plus_two_rule;"));

        Assert.assertTrue(result.stdout().contains("Number of AnotherSimpleView views 3"));
        Assert.assertTrue(
            result.stdout().contains("Ids of AnotherSimpleView first_id_SimpleWhiteboard;" +
                "first_id_SimpleWhiteboard;3;"));

        Assert.assertTrue(
            result.stdout().contains("Iterating over whiteboards with types " +
                "SimpleWhiteboard SimpleWhiteboard AnotherSimpleWhiteboard"));
        Assert.assertTrue(result.stdout().contains("Number of whiteboard is 3"));
        Assert.assertTrue(result.stdout().contains("First whiteboard type is SimpleWhiteboard"));

        Assert.assertTrue(result.stdout().contains("Number of whiteboard when date lower and upper "
            + "bounds are specified is 5"));
        Assert.assertTrue(result.stdout().contains("Number of whiteboard when date lower bound is "
            + "specified is 5"));
        Assert.assertTrue(result.stdout().contains("Number of whiteboard when date upper bounds "
            + "is specified is 5"));
        Assert.assertTrue(result.stdout().contains("Number of whiteboard when date interval is set "
            + "for the future is 0"));

        Assert.assertTrue(result.stdout().contains("string_field value in WhiteboardWithLzyMessageFields is fun6:fun7"));
        Assert.assertTrue(result.stdout().contains("int_field value in WhiteboardWithLzyMessageFields is 3"));
        Assert.assertTrue(result.stdout().contains("list_field length in WhiteboardWithLzyMessageFields is 3"));
        Assert.assertTrue(result.stdout().contains("optional_field value in WhiteboardWithLzyMessageFields is 1"));
        Assert.assertTrue(result.stdout().contains("inner_field value in WhiteboardWithLzyMessageFields is 6"));
        Assert.assertTrue(result.stdout().contains("enum_field value in WhiteboardWithLzyMessageFields is TestEnum.BAZ"));
        Assert.assertTrue(result.stdout().contains("non lzy message int field in WhiteboardWithLzyMessageFields is 3"));

        Assert.assertTrue(result.stdout().contains("string_field value in WhiteboardWithOneLzyMessageField is fun6:fun7"));
        Assert.assertTrue(result.stdout().contains("int_field value in WhiteboardWithOneLzyMessageField is 3"));

        Assert.assertFalse(result.stdout().contains("Could not create WhiteboardWithTwoLzyMessageFields because of a missing field"));
        Assert.assertTrue(result.stdout().contains("Could create WhiteboardWithTwoLzyMessageFields"));

        Assert.assertTrue(result.stdout().contains("Could not create WhiteboardWithLzyMessageFields because of a missing field"));
        Assert.assertFalse(result.stdout().contains("Could create WhiteboardWithLzyMessageFields"));

        Assert.assertTrue(result.stdout().contains("Value a in DefaultWhiteboard is 7, b length is 3, c is Hello, d is None"));
    }
}
