package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.util.concurrent.TimeUnit;

public class KuberRunTest extends LzyBaseTest {
    static class Config extends Defaults {
        private static final int    SERVANT_FS_PORT = 19999;
        private static final String KHARON_DOMAIN   = "kharon-lzy-prod.northeurope.cloudapp.azure.com";
        private static final String USER            = "phil";
        private static final String USER_KEY_PATH   = "/tmp/test-private.pem";
    }

    private static final String KHARON_DOMAIN_PROPERTY = "lzy.kharon.domain";
    protected LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
            Config.LZY_MOUNT,
            Config.SERVANT_PORT,
            Config.SERVANT_FS_PORT,
            String.format("http://%s:8899", System.getProperty(KHARON_DOMAIN_PROPERTY, Config.KHARON_DOMAIN)),
            Config.DEBUG_PORT,
            Config.USER,
            Config.USER_KEY_PATH
        );
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
                3. Task that requires GPU
         */
        // TODO: do we need to pass catboost as requirement here
        evalAndAssertScenarioResult( terminal, "catboost_integration_gpu");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Importing local modules
                2. Functions that return None
                3. Whiteboards/Views machinery
         */
        evalAndAssertScenarioResult(terminal, "whiteboards");
    }
}
