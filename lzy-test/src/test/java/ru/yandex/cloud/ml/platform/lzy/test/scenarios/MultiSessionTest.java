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
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;

public class MultiSessionTest extends LzyBaseTest {

    @Before
    public void setUp() {
        super.setUp();
    }

    private Terminal createTerminal(int port, int fsPort, int debugPort, String user, String mount) {
        final Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            mount,
            port,
            fsPort,
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
        final Terminal terminal1 = createTerminal(
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            FreePortFinder.find(23000, 24000),
            "user1",
            "/tmp/term1");

        final Terminal terminal2 = createTerminal(
            FreePortFinder.find(24000, 25000),
            FreePortFinder.find(25000, 26000),
            FreePortFinder.find(26000, 27000),
            "user2",
            "/tmp/term2");

        terminal1.execute(Map.of(), "bash", "-c", condaPrefix + "pip install catboost");
        terminal2.execute(Map.of(), "bash", "-c", condaPrefix + "pip install catboost");

        final String pyCommand = "python ../lzy-python/tests/scenarios/catboost_integration_cpu.py";

        //Act
        final CompletableFuture<Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result1.complete(
            terminal1.execute(Map.of("LZY_MOUNT", "/tmp/term1"), "bash", "-c", condaPrefix + pyCommand)));

        final CompletableFuture<Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result2.complete(
            terminal2.execute(Map.of("LZY_MOUNT", "/tmp/term2"), "bash", "-c", condaPrefix + pyCommand)));

        //Assert
        Assert.assertTrue(result1.get().stdout() + "\n\n" + result1.get().stderr(),
            result1.get().stdout().contains("Prediction: 1"));
        Assert.assertTrue(result2.get().stdout() + "\n\n" + result2.get().stderr(),
            result2.get().stdout().contains("Prediction: 1"));
    }

    @Test
    public void parallelPyGraphExecutionInSingleTerminal()
        throws ExecutionException, InterruptedException {
        final Terminal terminal = createTerminal(
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            FreePortFinder.find(22000, 23000),
            "user1",
            "/tmp/lzy");

        final String pyCommand = "python ../lzy-python/tests/scenarios/catboost_integration_cpu.py";

        //Act
        final CompletableFuture<Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result1.complete(
            terminal.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand)));

        final CompletableFuture<Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result2.complete(
            terminal.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand)));

        //Assert
        Assert.assertTrue(result1.get().stdout().contains("Prediction: 1"));
        Assert.assertTrue(result2.get().stdout().contains("Prediction: 1"));
    }
}
