package ru.yandex.cloud.ml.platform.lzy.test.scenarios.processes;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import ru.yandex.cloud.ml.platform.lzy.test.scenarios.FileIOOperation;

public class RunTest extends LzyBaseProcessTest {

    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext().startTerminalAtPathAndPort(
            defaultLzyMount(),
            9999,
            kharonContext().serverAddress(terminalContext().inDocker())
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            defaultTimeoutSec(),
            TimeUnit.SECONDS
        );
    }

    @Test
    public void testEcho42() {
        //Arrange
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42"
        );

        //Act
        terminal.publish(echo42.getName(), echo42);
        final ExecutionResult result = terminal.run(echo42.getName(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    public void testReadSlotToStdout() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/kek/some_file.txt";
        final String localFileName = defaultLzyMount() + "/lol/some_file.txt";
        final String channelName = "channel1";
        final FileIOOperation cat = new FileIOOperation(
            "cat_lzy",
            List.of(fileName),
            Collections.emptyList(),
            "cat $(echo $LZY_MOUNT)/" + fileName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c",
                "echo " + fileContent + " > " + localFileName));
        final ExecutionResult result = terminal.run(cat, Map.of(fileName, channelName), Map.of(),
            "");

        //Assert
        Assert.assertEquals(fileContent + "\n", result.stdout());
        Assert.assertTrue(terminal.pathExists(Path.of(localFileName)));
        Assert.assertEquals(fileContent + "\n",
            terminal.execute("bash", "-c", "cat " + localFileName).stdout());

        //Act
        terminal.destroyChannel(channelName);

        //Assert
        Assert.assertTrue(Utils.waitFlagUp(() -> !terminal.pathExists(Path.of(localFileName)),
            defaultTimeoutSec(), TimeUnit.SECONDS));
    }

    @Test
    public void testWriteToSlot() {
        //Arrange
        final String fileOutName = "/kek/some_file_out.txt";
        final String localFileOutName = defaultLzyMount() + "/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation echo_lzy = new FileIOOperation(
            "echo_lzy",
            Collections.emptyList(),
            List.of(fileOutName),
            "echo mama > $(echo $LZY_MOUNT)/" + fileOutName
        );

        //Act
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = terminal.execute("bash", "-c", "cat " + localFileOutName));
        final ExecutionResult result = terminal.run(
            echo_lzy,
            Map.of(fileOutName, channelOutName),
            Map.of(),
            ""
        );

        //Assert
        Assert.assertEquals("mama\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());
    }


    @Test
    public void testReadWrite() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/kek/some_file.txt";
        final String localFileName = defaultLzyMount() + "/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/kek/some_file_out.txt";
        final String localFileOutName = defaultLzyMount() + "/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat_to_file = new FileIOOperation(
            "cat_to_file_lzy",
            List.of(fileName),
            List.of(fileOutName),
            "cat $(echo $LZY_MOUNT)/" + fileName + " > $(echo $LZY_MOUNT)/" + fileOutName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c",
                "echo " + fileContent + " > " + localFileName));
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = terminal.execute("bash", "-c", "cat " + localFileOutName));
        final ExecutionResult result = terminal.run(
            cat_to_file,
            Map.of(
                fileName, channelName,
                fileOutName, channelOutName
            ),
            Map.of(),
            ""
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());
    }
}
