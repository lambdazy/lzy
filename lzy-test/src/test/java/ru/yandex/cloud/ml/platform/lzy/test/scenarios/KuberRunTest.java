package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyTerminalDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.TerminalThreadContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

public class KuberRunTest {

    private static final int DEFAULT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9999;
    private static final int DEFAULT_SERVANT_FS_PORT = 19999;
    private static final String LZY_MOUNT = "/tmp/lzy";
    private static final String TEST_USER = "phil";
    private static final String TEST_USER_KEY_PATH = "/tmp/test-private.pem";
    private static final String LZY_KHARON_DOMAIN_PROPERTY = "lzy.kharon.domain";
    private static final String DEFAULT_LZY_KHARON_DOMAIN = "kharon-lzy-prod.northeurope.cloudapp.azure.com";
    private final String LZY_KHARON_DOMAIN = System.getProperty(LZY_KHARON_DOMAIN_PROPERTY,
        DEFAULT_LZY_KHARON_DOMAIN);
    private final String SERVER_URL = String.format("http://%s:8899", LZY_KHARON_DOMAIN);
    private final LzyTerminalTestContext terminalContext = new TerminalThreadContext();
    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            DEFAULT_SERVANT_PORT,
            DEFAULT_SERVANT_FS_PORT,
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
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
                3. Task that requires GPU
         */

        //Arrange
        final String pyCommand = "python ../lzy-python/tests/scenarios/catboost_integration_gpu.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("Prediction: 1"));
    }

    @Test
    public void testUberGraph() {
        /* This scenario checks for:
                1. Importing local modules
                2. Functions that return None
                3. Whiteboards/Views machinery
         */

        //Arrange
        final String pyCommand = "python ../lzy-python/tests/scenarios/uber_graph.py";

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.execute(Map.of(),
            "bash", "-c",
            condaPrefix + pyCommand);

        //Assert
        Assert.assertTrue(result.stdout().contains("base echo"));
        Assert.assertTrue(result.stdout().contains("Just print some text"));

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
    }
}
