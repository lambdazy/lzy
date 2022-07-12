package ai.lzy.test.scenarios;

import ai.lzy.test.LzyTerminalTestContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.servant.agents.AgentStatus;

public class MultiSessionTest extends LocalScenario {
    private LzyTerminalTestContext.Terminal createTerminal(int port, int fsPort, int debugPort, String user, String mount) {
        final LzyTerminalTestContext.Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            mount,
            port,
            fsPort,
            kharonContext.serverAddress(),
            debugPort,
            user,
            null);
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            Config.TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        return terminal;
    }
    private static final Logger LOG = LogManager.getLogger(MultiSessionTest.class);

    @Test
    public void parallelPyGraphExecution() throws ExecutionException, InterruptedException {
        final String scenarioName = "catboost_integration_cpu";

        final String custom_mnt1 = "/tmp/term1";
        final LzyTerminalTestContext.Terminal terminal1 = createTerminal(
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            FreePortFinder.find(23000, 24000),
            "user1",
                custom_mnt1
            );
        final CompletableFuture<LzyTerminalTestContext.Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result1.complete(
            evalScenario(terminal1, scenarioName, List.of("catboost"), custom_mnt1)
        ));

        final String custom_mnt2 = "/tmp/term2";
        final LzyTerminalTestContext.Terminal terminal2 = createTerminal(
            FreePortFinder.find(24000, 25000),
            FreePortFinder.find(25000, 26000),
            FreePortFinder.find(26000, 27000),
            "user2",
            custom_mnt2);
        final CompletableFuture<LzyTerminalTestContext.Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result2.complete(
            evalScenario(terminal2, scenarioName, List.of("catboost"), custom_mnt2)
        ));

        assertWithExpected(scenarioName, result1.get());
        assertWithExpected(scenarioName, result2.get());
    }

}
