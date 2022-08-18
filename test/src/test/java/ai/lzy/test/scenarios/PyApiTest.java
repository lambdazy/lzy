package ai.lzy.test.scenarios;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.servant.agents.AgentStatus;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class PyApiTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
            Config.LZY_MOUNT,
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            kharonContext.serverAddress(),
            kharonContext.channelManagerProxyAddress(),
            FreePortFinder.find(22000, 23000),
            "testUser",
            terminalKeys.privateKeyPath().toString());
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            Config.TIMEOUT_SEC,
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
        evalAndAssertScenarioResult(terminal, "exec_fail");
    }

    @Test
    public void testEnvFail() {
        evalAndAssertScenarioResult(terminal, "env_fail");
    }

    @Test
    public void testCache() {
        evalAndAssertScenarioResult(terminal, "test_cache");
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */
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
        evalAndAssertScenarioResult(terminal, "whiteboards");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        evalAndAssertScenarioResult(terminal, "file_test");
    }
}