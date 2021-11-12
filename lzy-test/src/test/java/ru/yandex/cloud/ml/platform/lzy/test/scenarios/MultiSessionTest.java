package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MultiSessionTest extends LzyBaseTest {
    @Before
    public void setUp() {
        super.setUp();
    }

    private Terminal createTerminal(int port, int debugPort) {
        final Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            port,
            kharonContext.serverAddress(terminalContext.inDocker()),
            debugPort
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        return terminal;
    }

    @Test
    public void testEcho42() {
        final Terminal terminal1 = createTerminal(9998, 5006);
        final Terminal terminal2 = createTerminal(9999, 5007);

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
    public void parallelPyGraphExecution() {
        final Terminal terminal1 = createTerminal(9998, 5006);
        final Terminal terminal2 = createTerminal(9999, 5007);

        final String condaPrefix = "eval \"$(conda shell.bash hook)\" && " + "conda activate default && ";

        final AtomicReference<Terminal.ExecutionResult> result1 = new AtomicReference<>();
        ForkJoinPool.commonPool().execute(() -> {
            terminal1.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
            final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

            //Act
            result1.set(terminal1.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand));
        });

        final AtomicReference<Terminal.ExecutionResult> result2 = new AtomicReference<>();
        ForkJoinPool.commonPool().execute(() -> {
            terminal2.execute(Map.of(), "bash", "-c",
                condaPrefix + "pip install --default-timeout=100 /lzy-python setuptools");
            final String pyCommand = "python /lzy-python/examples/integration/simple_graph.py";

            //Act
            result2.set(terminal2.execute(Map.of(), "bash", "-c", condaPrefix + pyCommand));
        });

        terminal1.waitForShutdown(20, TimeUnit.MINUTES);
        terminal2.waitForShutdown(20, TimeUnit.MINUTES);

        //Assert
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result1.get().stdout()));
        Assert.assertEquals("More meaningful str than ever before3", Utils.lastLine(result2.get().stdout()));
    }
}
