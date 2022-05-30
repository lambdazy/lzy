package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;

public class LzyDryStartupTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        startTerminalWithDefaultConfig();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testFuseWorks() {
        //Assert
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/sbin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/bin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/dev")));
    }

    @Test
    public void testTerminalDiesAfterServerDied() {
        serverContext.close();

        //Assert
        Assert.assertTrue(terminal.waitForShutdown());
    }
}
