package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class TerminalCrashTest extends LzyBaseTest {

    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = createTerminal();
    }

    private LzyTerminalTestContext.Terminal createTerminal() {
        LzyTerminalTestContext.Terminal terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            9999,
            kharonContext.serverAddress(terminalContext.inDocker())
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
        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";
        final FileIOOperation cat = new FileIOOperation(
            "cat_lzy",
            List.of(fileName.substring(LZY_MOUNT.length())),
            Collections.emptyList(),
            "/tmp/lzy/sbin/cat " + fileName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        terminal.publish(cat.getName(), cat);
        ForkJoinPool.commonPool().execute(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            terminal.shutdownNow();
        });
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(
            cat.getName(),
            "",
            Map.of(fileName.substring(LZY_MOUNT.length()), channelName)
        );

        terminal = createTerminal();

        //Assert
        Assert.assertEquals(terminal.channelStatus(channelName), "");

        //Act
        terminal.destroyChannel(channelName);

        //Assert
        Assert.assertTrue(Utils.waitFlagUp(() ->
            !terminal.pathExists(Path.of(localFileName)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS));
    }
}
