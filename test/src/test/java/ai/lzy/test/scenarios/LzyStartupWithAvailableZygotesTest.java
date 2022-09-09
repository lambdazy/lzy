package ai.lzy.test.scenarios;

import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.v1.deprecated.Lzy;
import ai.lzy.v1.deprecated.LzyZygote;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

public class LzyStartupWithAvailableZygotesTest extends LocalScenario {
    @Test
    public void testRegisteredZygotesAvailable() {
        //Arrange
        final List<LzyZygote.Zygote> zygotes = IntStream.range(0, 10)
            .mapToObj(value -> LzyZygote.Zygote.newBuilder().setName("test_op_" + value).build())
            .toList();

        //noinspection ResultOfMethodCallIgnored
        zygotes.forEach(zygote -> serverContext.client().publish(
            Lzy.PublishRequest.newBuilder().setOperation(zygote).build())
        );
        startTerminalWithDefaultConfig();

        //Assert
        Assert.assertEquals(AgentStatus.EXECUTING, terminal.status());
        zygotes.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            Config.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }

    @Test
    public void testDuplicateRegister() {
        //Arrange
        final String opName = "test_op";
        //noinspection ResultOfMethodCallIgnored
        serverContext.client().publish(Lzy.PublishRequest.newBuilder()
            .setOperation(LzyZygote.Zygote.newBuilder().setName(opName).build())
            .build());
        startTerminalWithDefaultConfig();

        //Act/Assert
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(
            StatusRuntimeException.class,
            () -> serverContext.client().publish(Lzy.PublishRequest.newBuilder()
                .setOperation(LzyZygote.Zygote.newBuilder().setName(opName).build())
                .build())
        );
    }

    @Test
    public void testServantDoesNotSeeNewZygotes() {
        //Arrange
        final List<LzyZygote.Zygote> zygotesBeforeStart = IntStream.range(0, 10)
            .mapToObj(value -> LzyZygote.Zygote.newBuilder().setName("test_op_" + value).build())
            .toList();
        //noinspection ResultOfMethodCallIgnored
        zygotesBeforeStart.forEach(zygote -> serverContext.client().publish(
            Lzy.PublishRequest.newBuilder().setOperation(zygote).build())
        );
        startTerminalWithDefaultConfig();

        final List<LzyZygote.Zygote> zygotesAfterStart = IntStream.range(10, 20)
            .mapToObj(value -> LzyZygote.Zygote.newBuilder().setName("test_op_" + value).build())
            .toList();
        //noinspection ResultOfMethodCallIgnored
        zygotesAfterStart.forEach(zygote -> serverContext.client().publish(
            Lzy.PublishRequest.newBuilder().setOperation(zygote).build())
        );

        //Assert
        Assert.assertEquals(AgentStatus.EXECUTING, terminal.status());
        zygotesBeforeStart.forEach(registeredZygote -> Assert.assertTrue(terminal.pathExists(Paths.get(
            Config.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
        zygotesAfterStart.forEach(registeredZygote -> Assert.assertFalse(terminal.pathExists(Paths.get(
            Config.LZY_MOUNT + "/bin/" + registeredZygote.getName()))));
    }
}
