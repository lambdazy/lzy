package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

public class TerminalCrashTest extends LzyBaseTest {

    private LzyTerminalTestContext.Terminal createTerminal(String mount) {
        return createTerminal(FreePortFinder.find(20000, 30000), FreePortFinder.find(20000, 30000), mount);
    }

    private LzyTerminalTestContext.Terminal createTerminal(int port, int debugPort, String mount) {
        LzyTerminalTestContext.Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            mount,
            port,
            kharonContext.serverAddress(terminalContext.inDocker()),
            debugPort,
            LzyTerminalTestContext.TEST_USER,
            null
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        return terminal;
    }


    @Test
    public void testReadSlotToStdout() {
        //Arrange
        final LzyTerminalTestContext.Terminal terminal1 = createTerminal("/tmp/term1");
        final String fileName = "/tmp/lzy1/kek/some_file.txt";
        final String localFileName = "/tmp/term1/lol/some_file.txt";
        final String channelName = "channel1";
        final FileIOOperation cat = new FileIOOperation(
            "cat_lzy",
            List.of(fileName.substring("/tmp/lzy1".length())),
            Collections.emptyList(),
            "/tmp/lzy1/sbin/cat " + fileName,
            false
        );

        //Act
        terminal1.createChannel(channelName);
        terminal1.createSlot(localFileName, channelName, Utils.outFileSlot());
        terminal1.publish(cat.getName(), cat);
        ForkJoinPool.commonPool().execute(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            terminal1.shutdownNow();
        });
        terminal1.run(
            cat.getName(),
            "",
            Map.of(fileName.substring("/tmp/lzy1".length()), channelName)
        );

        LzyTerminalTestContext.Terminal terminal2 = createTerminal(FreePortFinder.find(20000, 30000), FreePortFinder.find(20000, 30000), "/tmp/term2");

        //Assert
        Assert.assertTrue(
            Utils.waitFlagUp(() -> {
                    final String tasksStatus = terminal2.tasksStatus();
                    return tasksStatus.equals("");
                },
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
            )
        );

        Assert.assertTrue(
            Utils.waitFlagUp(() -> {
                    final String channelStatus = terminal2.channelStatus(channelName);
                    return channelStatus.equals("Got exception while channel status (status_code=NOT_FOUND)\n");
                },
                DEFAULT_TIMEOUT_SEC,
                TimeUnit.SECONDS
            )
        );
        final String sessions = terminal2.sessions();
        try {
            final JsonNode node = new ObjectMapper().readTree(sessions);
            Assert.assertEquals(node.size(), 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Assert
        Assert.assertTrue(Utils.waitFlagUp(() ->
            !terminal2.pathExists(Path.of(localFileName)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Ignore
    @Test
    public void parallelExecutionOneTerminalFails() throws ExecutionException, InterruptedException {
        //Arrange
        final LzyTerminalTestContext.Terminal terminal1 = createTerminal(FreePortFinder.find(20000, 30000), FreePortFinder.find(20000, 30000), "");
        final LzyTerminalTestContext.Terminal terminal2 = createTerminal(FreePortFinder.find(20000, 30000), FreePortFinder.find(20000, 30000), "");
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42",
            false
        );
        terminal1.publish(echo42.getName(), echo42);
        terminal2.update();

        //Act
        ForkJoinPool.commonPool().execute(() -> terminal1.run(echo42.getName(), "", Map.of()));
        ForkJoinPool.commonPool().execute(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            terminal1.shutdownNow();
        });

        final CompletableFuture<ExecutionResult> result = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result.complete(terminal2.run(echo42.getName(), "", Map.of())));

        //Assert
        Assert.assertEquals("42\n", result.get().stdout());
    }
}
