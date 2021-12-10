package ru.yandex.cloud.ml.platform.lzy.test.scenarios.processes;

import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;
import ru.yandex.cloud.ml.platform.lzy.test.scenarios.LzyBaseDockerTest;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.PublishRequest;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.RegisteredZygote;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.Zygote;

public class LzyStartupTest extends LzyBaseDockerTest {

    @Test
    public void testFuseWorks() {
        //Arrange
        final Terminal terminal = terminalContext().startTerminalAtPathAndPort(
            defaultLzyMount(),
            defaultServantPort(),
            kharonContext().serverAddress(terminalContext().inDocker())
        );

        //Act
        terminal.waitForStatus(AgentStatus.EXECUTING, defaultTimeoutSec(), TimeUnit.SECONDS);

        //Assert
        Assert.assertTrue(terminal.pathExists(Paths.get(defaultLzyMount() + "/sbin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(defaultLzyMount() + "/bin")));
        Assert.assertTrue(terminal.pathExists(Paths.get(defaultLzyMount() + "/dev")));
    }

    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final List<RegisteredZygote> zygotes = IntStream.range(0, 10)
            .mapToObj(value -> serverContext().client().publish(PublishRequest.newBuilder()
                .setOperation(Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        final Terminal terminal = terminalContext().startTerminalAtPathAndPort(
            defaultLzyMount(),
            defaultServantPort(),
            kharonContext().serverAddress(terminalContext().inDocker())
        );
        final boolean status = terminal.waitForStatus(
            AgentStatus.EXECUTING,
            defaultTimeoutSec(),
            TimeUnit.SECONDS
        );

        //Assert
        Assert.assertTrue(status);
        zygotes.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            defaultLzyMount() + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testDuplicateRegister() {
        //Arrange
        final String opName = "test_op";
        //noinspection ResultOfMethodCallIgnored
        serverContext().client().publish(PublishRequest.newBuilder()
            .setOperation(Zygote.newBuilder().build())
            .setName(opName)
            .build());

        //Act/Assert
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(
            StatusRuntimeException.class,
            () -> serverContext().client().publish(PublishRequest.newBuilder()
                .setOperation(Zygote.newBuilder().build())
                .setName(opName)
                .build())
        );
    }

    @Test
    public void testServantDoesNotSeeNewZygotes() {
        //Arrange
        final List<Operations.RegisteredZygote> zygotesBeforeStart = IntStream.range(0, 10)
            .mapToObj(value -> serverContext().client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Act
        final LzyTerminalTestContext.Terminal terminal = terminalContext().startTerminalAtPathAndPort(
            defaultLzyMount(),
            defaultServantPort(),
            kharonContext().serverAddress(terminalContext().inDocker())
        );
        final boolean started = terminal.waitForStatus(
            AgentStatus.EXECUTING,
            defaultTimeoutSec(),
            TimeUnit.SECONDS
        );
        final List<Operations.RegisteredZygote> zygotesAfterStart = IntStream.range(10, 20)
            .mapToObj(value -> serverContext().client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(Operations.Zygote.newBuilder().build())
                .setName("test_op_" + value)
                .build()))
            .collect(Collectors.toList());

        //Assert
        Assert.assertTrue(started);
        zygotesBeforeStart.forEach(
            registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
                defaultLzyMount() + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(
            registeredZygote -> Assert.assertFalse(terminal.pathExists(Paths.get(
                defaultLzyMount() + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testServantDiesAfterServerDied() {
        //Arrange
        final LzyTerminalTestContext.Terminal terminal = terminalContext().startTerminalAtPathAndPort(
            defaultLzyMount(),
            defaultServantPort(),
            kharonContext().serverAddress(terminalContext().inDocker())
        );

        //Act
        final boolean started = terminal.waitForStatus(
            AgentStatus.EXECUTING,
            defaultTimeoutSec(),
            TimeUnit.SECONDS
        );
        serverContext().close();

        //Assert
        Assert.assertTrue(started);
        Assert.assertTrue(terminal.waitForShutdown(10, TimeUnit.SECONDS));
    }
}
