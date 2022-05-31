package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LzyStartupWithAvailableZygotesTest extends LocalScenario {
    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final List<Operations.RegisteredZygote> zygotes = IntStream.range(0, 10)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());
        startTerminalWithDefaultConfig();

        //Assert
        Assert.assertTrue(status);
        zygotes.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            Defaults.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testDuplicateRegister() {
        //Arrange
        final String opName = "test_op";
        //noinspection ResultOfMethodCallIgnored
        serverContext.client().publish(Lzy.PublishRequest.newBuilder()
            .setOperation(Operations.Zygote.newBuilder().build())
            .setName(opName)
            .build());
        startTerminalWithDefaultConfig();

        //Act/Assert
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(
            StatusRuntimeException.class,
            () -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName(opName)
                .build())
        );
    }

    @Test
    public void testServantDoesNotSeeNewZygotes() {
        //Arrange
        final List<Operations.RegisteredZygote> zygotesBeforeStart = IntStream.range(0, 10)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());
        startTerminalWithDefaultConfig();

        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(10, 20)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Assert
        Assert.assertTrue(status);
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            Defaults.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(terminal.pathExists(Paths.get(
            Defaults.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }
}
