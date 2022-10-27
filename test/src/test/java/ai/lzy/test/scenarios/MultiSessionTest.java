package ai.lzy.test.scenarios;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.test.LzyTerminalTestContext.Terminal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@Ignore
public class MultiSessionTest extends LocalScenario {

    private Terminal createTerminal(int port, int fsPort, int debugPort, String user, String mount) {
        final Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            mount,
            port,
            fsPort,
            kharonContext.serverAddress(),
            kharonContext.channelManagerProxyAddress(),
            debugPort,
            user,
            terminalKeys.privateKeyPath().toString());
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
        final Terminal terminal1 = createTerminal(
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            FreePortFinder.find(23000, 24000),
            "user1",
            custom_mnt1
        );
        final CompletableFuture<Terminal.ExecutionResult> result1 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result1.complete(
            evalScenario(terminal1, scenarioName, List.of("catboost"), custom_mnt1)
        ));

        final String custom_mnt2 = "/tmp/term2";
        final Terminal terminal2 = createTerminal(
            FreePortFinder.find(24000, 25000),
            FreePortFinder.find(25000, 26000),
            FreePortFinder.find(26000, 27000),
            "user2",
            custom_mnt2);
        final CompletableFuture<Terminal.ExecutionResult> result2 = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result2.complete(
            evalScenario(terminal2, scenarioName, List.of("catboost"), custom_mnt2)
        ));

        LOG.info("STDOUT1: {}", result1.get().stdout());
        LOG.info("STDERR1: {}", result1.get().stderr());

        LOG.info("STDOUT2: {}", result2.get().stdout());
        LOG.info("STDERR2: {}", result2.get().stderr());

        assertWithExpected(scenarioName, result1.get());
        assertWithExpected(scenarioName, result2.get());
    }

}
