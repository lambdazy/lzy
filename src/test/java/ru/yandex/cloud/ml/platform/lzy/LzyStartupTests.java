package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LzyStartupTests {
    private static final int DEFAULT_SERVANT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9889;
    private LzyTestContext context;

    @Before
    public void setUp() {
        context = new LzyLocalProcessesTestContext();
        context.start();
    }

    @After
    public void tearDown() {
        context.stop();
    }

    @Test
    public void testFuseWorks() {
        //Arrange
        final String lzyPath = "/tmp/lzy";

        //Act
        context.startTerminalAtPathAndPort(lzyPath, DEFAULT_SERVANT_PORT);

        //Assert
        Assert.assertTrue(context.waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, DEFAULT_SERVANT_PORT));
        Assert.assertTrue(context.pathExists(Paths.get(lzyPath + "/bin")));
        Assert.assertTrue(context.pathExists(Paths.get(lzyPath + "/sbin")));
        Assert.assertTrue(context.pathExists(Paths.get(lzyPath + "/dev")));
    }

    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final String lzyPath = "/tmp/lzy";
        final List<Operations.RegisteredZygote> zygotes = IntStream.range(0, 1000)
            .mapToObj(value -> context.server().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        context.startTerminalAtPathAndPort(lzyPath, DEFAULT_SERVANT_PORT);

        //Assert
        Assert.assertTrue(context.waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, DEFAULT_SERVANT_PORT));
        zygotes.forEach(registeredZygote -> Assert.assertTrue(context.pathExists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testDuplicateRegister() {
        //Arrange
        final String opName = "test_op";
        //noinspection ResultOfMethodCallIgnored
        context.server().publish(Lzy.PublishRequest.newBuilder()
            .setOperation(Operations.Zygote.newBuilder().build())
            .setName(opName)
            .build());

        //Act/Assert
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(
            StatusRuntimeException.class,
            () -> context.server().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName(opName)
                .build())
        );
    }

    @Test
    public void testServantDoesNotSeeNewZygotes() {
        //Arrange
        final String lzyPath = "/tmp/lzy";
        final List<Operations.RegisteredZygote> zygotesBeforeStart = IntStream.range(0, 1000)
            .mapToObj(value -> context.server().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        context.startTerminalAtPathAndPort(lzyPath, DEFAULT_SERVANT_PORT);
        context.waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, DEFAULT_SERVANT_PORT);
        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(1000, 2000)
            .mapToObj(value -> context.server().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());


        //Assert
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(context.pathExists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(context.pathExists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
    }
}
