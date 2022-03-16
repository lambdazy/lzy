package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

public class RunTest extends LzyBaseTest {
    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            9999,
            kharonContext.serverAddress(terminalContext.inDocker())
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    @Test
    public void testProcessEcho42() {
        //Arrange
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42",
            false
        );

        //Act
        terminal.publish(echo42.getName(), echo42);
        final ExecutionResult result = terminal.run(echo42.getName(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    @Ignore
    public void testDockerEcho42() {
        //Arrange
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42",
            true
        );

        //Act
        terminal.publish(echo42.getName(), echo42);
        final ExecutionResult result = terminal.run(echo42.getName(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    @Ignore
    public void testKeepAlive() {
        //Arrange
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "sleep 20m; echo 42",
            false
        );

        //Act
        terminal.publish(echo42.getName(), echo42);
        final ExecutionResult result = terminal.run(echo42.getName(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    public void testReadWrite() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/tmp/lzy/kek/some_file_out.txt";
        final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat_to_file = new FileIOOperation(
            "cat_to_file_lzy",
            List.of(fileName.substring(LZY_MOUNT.length())),
            List.of(fileOutName.substring(LZY_MOUNT.length())),
            "/tmp/lzy/sbin/cat " + fileName + " > " + fileOutName,
            false
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSlot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSlot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c",
                "echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file.getName(), cat_to_file);
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = terminal.execute("bash", "-c",
                "/tmp/lzy/sbin/cat " + localFileOutName));
        final ExecutionResult result = terminal.run(
            cat_to_file.getName(),
            "",
            Map.of(
                fileName.substring(LZY_MOUNT.length()), channelName,
                fileOutName.substring(LZY_MOUNT.length()), channelOutName
            )
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());
    }
}
