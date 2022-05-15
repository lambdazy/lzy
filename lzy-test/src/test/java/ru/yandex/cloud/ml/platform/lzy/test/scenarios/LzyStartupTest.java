package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.grpc.StatusRuntimeException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public class LzyStartupTest extends LocalScenario {
    private boolean status = false;

    @Before
    public void setUp() {
        super.setUp();
        startTerminalWithDefaultConfig();
    }

    @After
    public void tearDown() {
        super.tearDown();
        status = false;
    }

    @Test
    public void testFuseWorks() {
        //Assert
        Assert.assertTrue(terminal.pathExists(Paths.get(LZY_MOUNT + "/sbin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(LZY_MOUNT + "/bin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(LZY_MOUNT + "/dev")));
    }

    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final List<Operations.RegisteredZygote> zygotes = IntStream.range(0, 10)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());


        //Assert
        Assert.assertTrue(status);
        zygotes.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
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

        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(10, 20)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());


        //Assert
        Assert.assertTrue(status);
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(terminal.pathExists(Paths.get(
            LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testServantDiesAfterServerDied() {
        serverContext.close();

        //Assert
        Assert.assertTrue(status);
        Assert.assertTrue(terminal.waitForShutdown(10, TimeUnit.SECONDS));
    }
}
