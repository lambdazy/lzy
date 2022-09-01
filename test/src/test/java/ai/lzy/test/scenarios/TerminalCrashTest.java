package ai.lzy.test.scenarios;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.servant.agents.AgentStatus;
import ai.lzy.test.LzyTerminalTestContext;
import ai.lzy.test.TimeUtils;
import ai.lzy.test.impl.Utils;
import ai.lzy.v1.Tasks;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class TerminalCrashTest extends LocalScenario {

    private LzyTerminalTestContext.Terminal createTerminal(@SuppressWarnings("SameParameterValue") String mount) {
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
            kharonContext.channelManagerProxyAddress(),
            debugPort,
            LzyTerminalTestContext.TEST_USER,
            terminalKeys.privateKeyPath().toString());
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            Config.TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        return terminal;
    }

    @Test
    public void testReadSlotToStdout() throws InterruptedException {
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
        final CountDownLatch terminalFailsLatch = new CountDownLatch(1);

        //Act
        final String channelId = terminal1.createChannel(channelName);
        terminal1.createSlot(localFileName, channelId, Utils.outFileSlot());
        terminal1.publish(cat);
        ForkJoinPool.commonPool().execute(() -> {
            TimeUtils.waitFlagUp(() -> {
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
            terminalFailsLatch.countDown();
        });
        terminal1.run(
            cat.name(),
            "",
            Map.of(fileName.substring("/tmp/lzy1".length()), channelId)
        );

        LzyTerminalTestContext.Terminal terminal2 = createTerminal(
            FreePortFinder.find(23000, 23100),
            FreePortFinder.find(23100, 23200),
            FreePortFinder.find(23200, 23300),
            "/tmp/term2");

        //Assert
        Assert.assertTrue(
            TimeUtils.waitFlagUp(() -> getTaskStatusByName(terminal2, cat.name()) == null,
                Config.TIMEOUT_SEC,
                TimeUnit.SECONDS
            )
        );

        var exception = new AtomicReference<Exception>(null);
        Assert.assertTrue(
            TimeUtils.waitFlagUp(() -> {
                    try {
                        final String channelStatus = terminal2.channelStatus(channelId);
                        return channelStatus.equals("Got exception while channel status (status_code=NOT_FOUND)\n");
                    } catch (LzyTerminalTestContext.TerminalCommandFailedException e) {
                        if (e.getResult().stdout().equals(
                                "Got exception while channel status (status_code=NOT_FOUND)\n"))
                        {
                            return true;
                        }
                        exception.set(e);
                        return true;
                    } catch (Exception e) {
                        exception.set(e);
                        return true;
                    }
                },
                Config.TIMEOUT_SEC,
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

        final String cs = terminal2.channelsStatus();
        Assert.assertEquals("", cs);
        Assert.assertTrue(terminalFailsLatch.await(Config.TIMEOUT_SEC, TimeUnit.SECONDS));
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
        final FileIOOperation sleep = new FileIOOperation(
            "sleep",
            Collections.emptyList(),
            Collections.emptyList(),
            "sleep 600"
        );
        final FileIOOperation echo43 = new FileIOOperation(
            "echo43",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 43"
        );
        terminal1.publish(sleep);
        terminal1.publish(echo43);
        terminal2.update();

        final CountDownLatch terminalFailsLatch = new CountDownLatch(1);

        //Act
        ForkJoinPool.commonPool().execute(() -> terminal1.run(sleep.name(), "", Map.of()));
        ForkJoinPool.commonPool().execute(() -> {
            TimeUtils.waitFlagUp(() -> {
                    final Tasks.TaskStatus task = getTaskStatusByName(terminal1, sleep.name());
                    if (task == null) {
                        return false;
                    }
                    return task.getStatus().getNumber() >= Tasks.TaskProgress.Status.EXECUTING.getNumber();
                },
                Config.TIMEOUT_SEC,
                TimeUnit.SECONDS
            );
            terminal1.shutdownNow();
            terminalFailsLatch.countDown();
        });

        final CompletableFuture<LzyTerminalTestContext.Terminal.ExecutionResult> result = new CompletableFuture<>();
        ForkJoinPool.commonPool().execute(() -> result.complete(terminal2.run(echo43.name(), "", Map.of())));

        //Assert
        Assert.assertTrue(terminalFailsLatch.await(Config.TIMEOUT_SEC, TimeUnit.SECONDS));
        Assert.assertEquals("43\n", result.get().stdout());
    }

    @Test
    public void testLongExecution() {
        //Arrange
        final LzyTerminalTestContext.Terminal terminal = createTerminal(
            FreePortFinder.find(20000, 20100),
            FreePortFinder.find(20100, 20200),
            FreePortFinder.find(20200, 20300),
            "/tmp/lzy");
        final LzyTerminalTestContext.Terminal terminal2 = createTerminal(
            FreePortFinder.find(20300, 20400),
            FreePortFinder.find(20400, 20500),
            FreePortFinder.find(20500, 20600),
            "/tmp/term2");

        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy1/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/tmp/lzy1/kek/some_file_out.txt";
        final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat = new FileIOOperation(
            "cat_to_file_lzy",
            List.of(fileName.substring("/tmp/lzy1".length())),
            List.of(fileOutName.substring("/tmp/lzy1".length())),
            "sleep 600; /tmp/lzy1/sbin/cat " + fileName + " > " + fileOutName
        );

        final String channelId = terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelId, Utils.outFileSlot());
        final String channelOutId = terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutId, Utils.inFileSlot());
        terminal.publish(cat);

        //Act
        String csBefore = terminal.channelsStatus();
        ForkJoinPool.commonPool().execute(() -> terminal.execute("echo " + fileContent + " > " + localFileName));
        ForkJoinPool.commonPool().execute(() -> terminal.run(
            cat.name(),
            "",
            Map.of(
                fileName.substring("/tmp/lzy1".length()), channelId,
                fileOutName.substring("/tmp/lzy1".length()), channelOutId
            )
        ));

        TimeUtils.waitFlagUp(() -> {
                final Tasks.TaskStatus task = getTaskStatusByName(terminal, cat.name());
                if (task == null) {
                    return false;
                }
                return task.getStatus().getNumber() >= Tasks.TaskProgress.Status.EXECUTING.getNumber();
            },
            Config.TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
        terminal.shutdownNow();
        terminal.waitForShutdown();

        final boolean flagUp = TimeUtils.waitFlagUp(() -> terminal2.channelsStatus().equals(""), Config.TIMEOUT_SEC,
            TimeUnit.SECONDS);

        //Assert
        Assert.assertNotEquals("", csBefore);
        Assert.assertTrue(flagUp);
    }
}
