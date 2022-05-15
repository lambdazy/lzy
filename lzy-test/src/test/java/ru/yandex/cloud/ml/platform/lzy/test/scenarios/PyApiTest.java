package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class PyApiTest extends LocalScenario {
    private static final Logger LOG = LogManager.getLogger(PyApiTest.class);

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                FreePortFinder.find(20000, 21000),
                FreePortFinder.find(21000, 22000),
                kharonContext.serverAddress(),
                FreePortFinder.find(22000, 23000),
                "testUser",
                null);
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
        runAndCompareWithExpectedFile(List.of("catboost"), "catboost_integration_cpu", LOG);
    }

    @Test
    public void testExecFail() {
        //Arrange
        runAndCompareWithExpectedFile("exec_fail", LOG);
    }

    @Test
    public void testEnvFail() {
        //Arrange
        runAndCompareWithExpectedFile("env_fail", LOG);
    }

    @Test
    public void testCache() {
        //Arrange
        runAndCompareWithExpectedFile("test_cache", LOG);
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */

        //Arrange
        runAndCompareWithExpectedFile("import", LOG);
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        runAndCompareWithExpectedFile("none_result", LOG);
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        //Arrange
        runAndCompareWithExpectedFile("whiteboards", LOG);
    }
}