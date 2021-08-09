package ru.yandex.cloud.ml.platform.lzy;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class LzyStartupTests extends LzyBaseTest {
    private static final int DEFAULT_FUSE_TIMEOUT_SEC = 10;

    @Test
    public void testFuseWorks() throws Exception {
        final String lzyPath = "/tmp/lzy";
        startTerminalAtPath(lzyPath);
        Assert.assertTrue(waitForFuse(lzyPath, DEFAULT_FUSE_TIMEOUT_SEC, TimeUnit.SECONDS));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/bin")));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/sbin")));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/dev")));
    }
}
