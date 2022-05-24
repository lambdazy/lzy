package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TerminalCrashTest extends LocalScenario {
    private LzyTerminalTestContext.Terminal createTerminal(String mount) {
        return createTerminal(
            FreePortFinder.find(20000, 21000),
            FreePortFinder.find(21000, 22000),
            FreePortFinder.find(22000, 23000),
            mount);
    }

    private LzyTerminalTestContext.Terminal createTerminal(int port, int fsPort, int debugPort, String mount) {
        LzyTerminalTestContext.Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            mount,
            port,
            fsPort,
            kharonContext.serverAddress(),
            debugPort,
            LzyTerminalTestContext.TEST_USER,
            null
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            Utils.Defaults.TIMEOUT_SEC,
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
            "/tmp/lzy1/sbin/cat " + fileName
        );

        //Act
        terminal1.createChannel(channelName);
        terminal1.createSlot(localFileName, channelName, Utils.outFileSlot());
        terminal1.publish(cat);
        ForkJoinPool.commonPool().execute(() -> {
            Utils.waitFlagUp(() -> {
                    final Tasks.TaskStatus task = getTaskStatusByName(terminal1, cat.name());
                    if (task == null) {
                        return false;
                    }
                    return task.getStatus().getNumber() >= Tasks.TaskProgress.Status.EXECUTING.getNumber();
                },
                Config.TIMEOUT_SEC,
                TimeUnit.SECONDS
            );
            terminal1.shutdownNow();
            terminal1.waitForShutdown();
        });
        terminal1.run(
            cat.name(),
            "",
            Map.of(fileName.substring("/tmp/lzy1".length()), channelName)
        );

        LzyTerminalTestContext.Terminal terminal2 = createTerminal(
            FreePortFinder.find(23000, 23100),
            FreePortFinder.find(23100, 23200),
            FreePortFinder.find(23200, 23300),
            "/tmp/term2");

        //Assert
        Assert.assertTrue(
            Utils.waitFlagUp(() -> getTaskStatusByName(terminal2, cat.name()) == null,
                Utils.Defaults.TIMEOUT_SEC,
                TimeUnit.SECONDS
            )
        );

        var exception = new AtomicReference<Exception>(null);
        Assert.assertTrue(
            Utils.waitFlagUp(() -> {
                    try {
                        final String channelStatus = terminal2.channelStatus(channelName);
                        return channelStatus.equals("Got exception while channel status (status_code=NOT_FOUND)\n");
                    } catch (LzyTerminalTestContext.TerminalCommandFailedException e) {
                        if (e.getResult().stdout().equals(
                                "Got exception while channel status (status_code=NOT_FOUND)\n")) {
                            return true;
                        }
                        exception.set(e);
                        return true;
                    } catch (Exception e) {
                        exception.set(e);
                        return true;
                    }
                },
                Utils.Defaults.TIMEOUT_SEC,
                TimeUnit.SECONDS
            )
        );
        Assert.assertNull(exception.get());
        final String sessions = terminal2.sessions();
        try {
            final JsonNode node = new ObjectMapper().readTree(sessions);
            Assert.assertEquals(node.size(), 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Assert
        Assert.assertTrue(Utils.waitFlagUp(() ->
            !terminal2.pathExists(Path.of(localFileName)), Utils.Defaults.TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Nullable
    private Tasks.TaskStatus getTaskStatusByName(LzyTerminalTestContext.Terminal terminal, String operationNamePrefix) {
        final String tasksStatus = terminal.tasksStatus();
        System.out.println("tasksStatus");
        System.out.println(tasksStatus);

        try {
            final Tasks.TasksList.Builder tasksBuilder = Tasks.TasksList.newBuilder();
            JsonFormat.parser().merge(tasksStatus, tasksBuilder);
            final Tasks.TasksList tasksList = tasksBuilder.build();
            return tasksList.getTasksList().stream()
                    .filter(task -> task.getZygote().getName().startsWith(operationNamePrefix))
                    .findFirst().orElse(null);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void parallelExecutionOneTerminalFails() throws ExecutionException, InterruptedException {
        //Arrange
        final LzyTerminalTestContext.Terminal terminal1 = createTerminal(
            FreePortFinder.find(20000, 20100),
            FreePortFinder.find(20100, 20200),
            FreePortFinder.find(20200, 20300),
            "/tmp/term1");
        final LzyTerminalTestContext.Terminal terminal2 = createTerminal(
            FreePortFinder.find(20300, 20400),
            FreePortFinder.find(20400, 20500),
            FreePortFinder.find(20500, 20600),
            "/tmp/term2");
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42"
        );
        final FileIOOperation echo43 = new FileIOOperation(
                "echo43",
                Collections.emptyList(),
                Collections.emptyList(),
                "echo 43",
                false
        );
        terminal1.publish(echo42);
        terminal1.publish(echo43);
        terminal2.update();

        //Act
        ForkJoinPool.commonPool().execute(() -> terminal1.run(echo42.name(), "", Map.of()));
        ForkJoinPool.commonPool().execute(() -> {
            Utils.waitFlagUp(() -> {
                    final Tasks.TaskStatus task = getTaskStatusByName(terminal1, echo42.name());
                    if (task == null) {
                        return false;
                    }
                    return task.getStatus().getNumber() >= Tasks.TaskProgress.Status.EXECUTING.getNumber();
                },
                Utils.Defaults.TIMEOUT_SEC,
                TimeUnit.SECONDS
            );
            terminal1.shutdownNow();
        });

        final CompletableFuture<ExecutionResult> result = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result.complete(terminal2.run(echo43.name(), "", Map.of())));

        //Assert
        Assert.assertEquals("43\n", result.get().stdout());
    }

    @Test
    public void testLongExecution() {
        final LzyTerminalTestContext.Terminal terminal = createTerminal(
            FreePortFinder.find(20000, 20100),
            FreePortFinder.find(20100, 20200),
            FreePortFinder.find(20200, 20300),
            "/tmp/lzy");

        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "sleep 600; echo 42"
        );

        //Act
        terminal.publish(echo42);
        Thread t = new Thread(() -> terminal.run(echo42.name(), "", Map.of()));
        t.start();
        Utils.waitFlagUp(() -> {
                final Tasks.TaskStatus task = getTaskStatusByName(terminal, echo42.name());
                if (task == null) {
                    return false;
                }
                return task.getStatus().getNumber() >= Tasks.TaskProgress.Status.EXECUTING.getNumber();
            },
            Utils.Defaults.TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        terminal.shutdownNow();
        terminal.waitForShutdown();
    }
}
