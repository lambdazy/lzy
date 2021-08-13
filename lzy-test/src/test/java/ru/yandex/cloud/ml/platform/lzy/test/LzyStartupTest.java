package ru.yandex.cloud.ml.platform.lzy.test;

import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServantProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LzyStartupTest {
    private static final int DEFAULT_SERVANT_INIT_TIMEOUT_SEC = 30;
    private static final int DEFAULT_SERVANT_PORT = 9999;

    private LzyServantTestContext servantContext;
    private LzyServerTestContext serverContext;

    @Before
    public void setUp() {
        servantContext = new LzyServantProcessesContext();
        serverContext = new LzyServerProcessesContext();
    }

    @After
    public void tearDown() {
        servantContext.close();
        serverContext.close();
    }

    @Test
    public void testFuseWorks() {
        //Arrange
        final String lzyPath = "/tmp/lzy";

        //Act
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
            lzyPath,
            DEFAULT_SERVANT_PORT,
            serverContext.host(servantContext.inDocker()),
            serverContext.port()
        );
        servant.waitForStatus(ServantStatus.EXECUTING, DEFAULT_SERVANT_INIT_TIMEOUT_SEC, TimeUnit.SECONDS);

        //Assert
        Assert.assertTrue(servant.pathExists(Paths.get(lzyPath + "/sbin")));
        Assert.assertTrue(servant.pathExists(Paths.get(lzyPath + "/bin")));
        Assert.assertTrue(servant.pathExists(Paths.get(lzyPath + "/dev")));
    }

    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final String lzyPath = "/tmp/lzy";

        //Act
        final List<Operations.RegisteredZygote> zygotes = IntStream.range(0, 10)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
            lzyPath,
            DEFAULT_SERVANT_PORT,
            serverContext.host(servantContext.inDocker()),
            serverContext.port()
        );

        //Assert
        Assert.assertTrue(servant.waitForStatus(
            ServantStatus.EXECUTING,
            DEFAULT_SERVANT_INIT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        ));
        zygotes.forEach(registeredZygote -> Assert.assertTrue(servant.pathExists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
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
        final String lzyPath = "/tmp/lzy";
        final List<Operations.RegisteredZygote> zygotesBeforeStart = IntStream.range(0, 10)
            .mapToObj(value -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
            lzyPath,
            DEFAULT_SERVANT_PORT,
            serverContext.host(servantContext.inDocker()),
            serverContext.port()
        );
        final boolean started = servant.waitForStatus(
            ServantStatus.EXECUTING,
            DEFAULT_SERVANT_INIT_TIMEOUT_SEC,
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
            lzyPath + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(servant.pathExists(Paths.get(
            lzyPath + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testServantDiesAfterServerDied() {
        //Arrange
        final String lzyPath = "/tmp/lzy";

        //Act
        final LzyServantTestContext.Servant servant = servantContext.startTerminalAtPathAndPort(
            lzyPath,
            DEFAULT_SERVANT_PORT,
            serverContext.host(servantContext.inDocker()),
            serverContext.port()
        );
        final boolean started = servant.waitForStatus(
            ServantStatus.EXECUTING,
            DEFAULT_SERVANT_INIT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        serverContext.close();

        //Assert
        Assert.assertTrue(started);
        Assert.assertTrue(servant.waitForShutdown(10, TimeUnit.SECONDS));
    }
}
