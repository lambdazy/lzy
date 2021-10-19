package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LzyStartupTest extends LzyBaseTest {
    @Test
    public void testFuseWorks() {
        //Arrange
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                DEFAULT_SERVANT_PORT,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );

        //Act
        servant.waitForStatus(ServantStatus.EXECUTING, DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS);

        //Assert
        Assert.assertTrue(servant.pathExists(Paths.get(LZY_MOUNT + "/sbin")));
        Assert.assertTrue(servant.pathExists(Paths.get(LZY_MOUNT + "/bin")));
        Assert.assertTrue(servant.pathExists(Paths.get(LZY_MOUNT + "/dev")));
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

        //Act
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                DEFAULT_SERVANT_PORT,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );
        final boolean status = servant.waitForStatus(
                ServantStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );

        //Assert
        Assert.assertTrue(status);
        zygotes.forEach(registeredZygote -> Assert.assertTrue(servant.pathExists(Paths.get(
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

        //Act
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                DEFAULT_SERVANT_PORT,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );
        final boolean started = servant.waitForStatus(
                ServantStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(10, 20)
                .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                        .setOperation(Operations.Zygote.newBuilder().build())
                        .setName("test_op_" + value)
                        .build()))
                .collect(Collectors.toList());


        //Assert
        Assert.assertTrue(started);
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(servant.pathExists(Paths.get(
                LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(servant.pathExists(Paths.get(
                LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testServantDiesAfterServerDied() {
        //Arrange
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                DEFAULT_SERVANT_PORT,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );

        //Act
        final boolean started = servant.waitForStatus(
                ServantStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
        );
        serverContext.close();

        //Assert
        Assert.assertTrue(started);
        Assert.assertTrue(servant.waitForShutdown(10, TimeUnit.SECONDS));
    }
}
