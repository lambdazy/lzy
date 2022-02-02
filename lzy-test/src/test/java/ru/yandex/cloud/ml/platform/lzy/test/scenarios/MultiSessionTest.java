package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

public class MultiSessionTest extends LzyBaseTest {

    @Before
    public void setUp() {
        super.setUp();
    }

    private Terminal createTerminal(int port, int debugPort, String user) {
        final Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            port,
            kharonContext.serverAddress(terminalContext.inDocker()),
            debugPort,
            user,
            null);
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        return terminal;
    }

    @Test
    public void parallelPyGraphExecution() throws ExecutionException, InterruptedException {
        //Arrange
        final Terminal terminal1 = createTerminal(9998, 5006, "user1");
        final Terminal terminal2 = createTerminal(9999, 5007, "user2");
        final String pyCommand = "python /lzy-python/tests/scenarios/simple_graph_cpu.py";

        //Act
        final CompletableFuture<Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result1.complete(
            terminal1.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand)));

        final CompletableFuture<Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result2.complete(
            terminal2.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand)));

        //Assert
        Assert.assertEquals("Prediction: 1",
            Utils.lastLine(result1.get().stdout()));
        Assert.assertEquals("Prediction: 1",
            Utils.lastLine(result2.get().stdout()));
    }
}
