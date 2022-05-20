package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.util.concurrent.TimeUnit;

public class KuberRunTest extends LzyBaseTest {
    private static final int    DEFAULT_SERVANT_FS_PORT    = 19999;
    private static final String DEFAULT_LZY_KHARON_DOMAIN  = "kharon-lzy-prod.northeurope.cloudapp.azure.com";
    private static final String TEST_USER                  = "phil";
    private static final String TEST_USER_KEY_PATH         = "/tmp/test-private.pem";
    private static final String LZY_KHARON_DOMAIN_PROPERTY = "lzy.kharon.domain";

    private static final Logger LOG                        = LogManager.getLogger(KuberRunTest.class);
    protected LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            DEFAULT_SERVANT_PORT,
            DEFAULT_SERVANT_FS_PORT,
            String.format("http://%s:8899", System.getProperty(LZY_KHARON_DOMAIN_PROPERTY, DEFAULT_LZY_KHARON_DOMAIN)),
            DEFAULT_DEBUG_PORT,
            TEST_USER,
            TEST_USER_KEY_PATH
        );
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
                3. Task that requires GPU
         */
        // TODO: do we need to pass catboost as requirement here
        runAndCompareWithExpectedFile( "catboost_integration_gpu", LOG, terminal);
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Importing local modules
                2. Functions that return None
                3. Whiteboards/Views machinery
         */
        runAndCompareWithExpectedFile("whiteboards", LOG, terminal);
    }
}
