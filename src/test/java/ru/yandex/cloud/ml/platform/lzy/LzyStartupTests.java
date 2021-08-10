package ru.yandex.cloud.ml.platform.lzy;

import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LzyStartupTests extends LzyBaseTest {
    private static final int DEFAULT_SERVANT_TIMEOUT_SEC = 30;

    @Test
    public void testFuseWorks() throws Exception {
        //Arrange
        final String lzyPath = "/tmp/lzy";

        //Act
        startTerminalAtPath(lzyPath);

        //Assert
        Assert.assertTrue(waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, 9999));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/bin")));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/sbin")));
        Assert.assertTrue(Files.exists(Paths.get(lzyPath + "/dev")));
    }

    @Test
    public void testRegisteredZygotesAvailable() throws Exception {
        //Arrange
        final String lzyPath = "/tmp/lzy";
        final List<Operations.RegisteredZygote> zygotes = IntStream.range(0, 1000)
            .mapToObj(value -> lzyServerClient.publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        startTerminalAtPath(lzyPath);

        //Assert
        Assert.assertTrue(waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, 9999));
        zygotes.forEach(registeredZygote -> Assert.assertTrue(Files.exists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testDuplicateRegister() {
        //Arrange
        final String opName = "test_op";
        //noinspection ResultOfMethodCallIgnored
        lzyServerClient.publish(Lzy.PublishRequest.newBuilder()
            .setOperation(Operations.Zygote.newBuilder().build())
            .setName(opName)
            .build());

        //Act/Assert
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(StatusRuntimeException.class, () -> lzyServerClient.publish(Lzy.PublishRequest.newBuilder()
            .setOperation(Operations.Zygote.newBuilder().build())
            .setName(opName)
            .build()));
    }

    @Test
    public void testServantDoesNotSeeNewZygotes() throws Exception {
        //Arrange
        final String lzyPath = "/tmp/lzy";
        final List<Operations.RegisteredZygote> zygotesBeforeStart = IntStream.range(0, 1000)
            .mapToObj(value -> lzyServerClient.publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        startTerminalAtPath(lzyPath);
        waitForServants(DEFAULT_SERVANT_TIMEOUT_SEC, TimeUnit.SECONDS, 9999);
        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(1000, 2000)
            .mapToObj(value -> lzyServerClient.publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());


        //Assert
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(Files.exists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(Files.exists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
    }
}
