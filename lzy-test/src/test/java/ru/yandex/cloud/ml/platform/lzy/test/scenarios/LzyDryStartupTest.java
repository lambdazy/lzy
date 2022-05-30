package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LzyDryStartupTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        startTerminalWithDefaultConfig();
    }

    @After
    public void tearDown() {
        super.tearDown();
        stopTerminal();
    }

    @Test
    public void testFuseWorks() {
        //Assert
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/sbin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/bin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(Defaults.LZY_MOUNT + "/dev")));
    }

    @Test
    public void testServantDiesAfterServerDied() {
        serverContext.close();

        //Assert
        Assert.assertTrue(isExecuting);
        Assert.assertTrue(terminal.waitForShutdown(10, TimeUnit.SECONDS));
    }
}
