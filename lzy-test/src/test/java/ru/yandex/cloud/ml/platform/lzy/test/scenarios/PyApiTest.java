package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;


public class PyApiTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
                Utils.Defaults.LZY_MOUNT,
                FreePortFinder.find(20000, 21000),
                FreePortFinder.find(21000, 22000),
                kharonContext.serverAddress(),
                FreePortFinder.find(22000, 23000),
                "testUser",
                null);
        terminal.waitForStatus(
                AgentStatus.EXECUTING,
                Utils.Defaults.TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        evalAndAssertScenarioResult(terminal, "catboost_integration_cpu", List.of("catboost"));
    }

    @Test
    public void testExecFail() {
        //Arrange
        evalAndAssertScenarioResult(terminal, "exec_fail");
    }

    @Test
    public void testEnvFail() {
        //Arrange
        evalAndAssertScenarioResult(terminal, "env_fail");
    }

    @Test
    public void testCache() {
        //Arrange
        evalAndAssertScenarioResult(terminal, "test_cache");
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */

        //Arrange
        evalAndAssertScenarioResult(terminal, "import");
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        evalAndAssertScenarioResult(terminal, "none_result");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        //Arrange
        evalAndAssertScenarioResult(terminal, "whiteboards");
    }
}