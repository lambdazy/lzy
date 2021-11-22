package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

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
    public void testEcho42() {
        final Terminal terminal1 = createTerminal(9998, 5006, "user1");
        final Terminal terminal2 = createTerminal(9999, 5007, "user2");

        //Arrange
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42"
        );

        //Act
        terminal1.publish(echo42.getName(), echo42);
        final Terminal.ExecutionResult result1 = terminal1.run(echo42.getName(), "", Map.of());
        terminal2.update();
        final Terminal.ExecutionResult result2 = terminal2.run(echo42.getName(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result1.stdout());
        Assert.assertEquals("42\n", result2.stdout());
    }

    @Test
    public void parallelPyGraphExecution() throws ExecutionException, InterruptedException {
        final Terminal terminal1 = createTerminal(9998, 5006, "user1");
        final Terminal terminal2 = createTerminal(9999, 5007, "user2");

        final String condaPrefix = "eval \"$(conda shell.bash hook)\" && " + "conda activate default && ";

        final CompletableFuture<Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            terminal1.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
            final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

            //Act
            result1.complete(terminal1.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand));
        });

        final CompletableFuture<Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> {
            terminal2.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
            final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

            //Act
            result2.complete(terminal2.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand));
        });

        //Assert
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result1.get().stdout()));
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result2.get().stdout()));
    }
}
