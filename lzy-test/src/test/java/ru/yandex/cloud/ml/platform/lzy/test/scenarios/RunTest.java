package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext.Servant.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("CommentedOutCode")
public class RunTest extends LzyBaseTest {
    private LzyServantTestContext.Servant terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = servantContext.startTerminalAtPathAndPort(
                LZY_MOUNT,
                9999,
                serverContext.host(servantContext.inDocker()),
                serverContext.port()
        );
        terminal.waitForStatus(
                ServantStatus.EXECUTING,
                DEFAULT_TIMEOUT_SEC,
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
        final String fileName = "/tmp/lzy/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";
        final FileIOOperation cat = new FileIOOperation(
                "cat_lzy",
                List.of(fileName.substring(LZY_MOUNT.length())),
                Collections.emptyList(),
                "cat " + fileName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        ForkJoinPool.commonPool()
                .execute(() -> terminal.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        terminal.publish(cat.getName(), cat);
        final ExecutionResult result = terminal.run(
                cat.getName(),
                "",
                Map.of(fileName.substring(LZY_MOUNT.length()), channelName)
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result.stdout());
        Assert.assertTrue(terminal.pathExists(Path.of(localFileName)));

        //Act
        terminal.destroyChannel(channelName);

        //Assert
        Assert.assertTrue(Utils.waitFlagUp(() ->
                !terminal.pathExists(Path.of(localFileName)), DEFAULT_TIMEOUT_SEC, TimeUnit.SECONDS));
    }

    @Test
    public void testWriteToSlot() {
        //Arrange
        final String fileOutName = "/tmp/lzy/kek/some_file_out.txt";
        final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation echo_lzy = new FileIOOperation(
                "echo_lzy",
                Collections.emptyList(),
                List.of(fileOutName.substring(LZY_MOUNT.length())),
                "echo mama > " + fileOutName
        );

        //Act
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        terminal.publish(echo_lzy.getName(), echo_lzy);
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
                .execute(() -> result1[0] = terminal.execute("bash", "-c", "cat " + localFileOutName));
        final ExecutionResult result = terminal.run(
                echo_lzy.getName(),
                "",
                Map.of(
                        fileOutName.substring(LZY_MOUNT.length()), channelOutName
                )
        );

        //Assert
        Assert.assertEquals("mama\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());
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
                "cat " + fileName + " > " + fileOutName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        ForkJoinPool.commonPool()
                .execute(() -> terminal.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file.getName(), cat_to_file);
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
                .execute(() -> result1[0] = terminal.execute("bash", "-c", "cat " + localFileOutName));
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

    //@Test
    //public void testStartupPy() {
    //    //Arrange
    //    final String fileContent = "fileContent";
    //    final String fileName = "/tmp/lzy/kek/some_file.txt";
    //    final String localFileName = "/tmp/lzy/lol/some_file.txt";
    //    final String channelName = "channel1";
    //
    //    final String fileOutName = "/tmp/lzy/kek/some_file_out.txt";
    //    final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
    //    final String channelOutName = "channel2";
    //
    //    final FileIOOperation python_io_file_lzy = new FileIOOperation(
    //        "python_io_file_lzy",
    //        List.of(fileName.substring(LZY_MOUNT.length())),
    //        List.of(fileOutName.substring(LZY_MOUNT.length())),
    //        "python3 /lzy-python/src/main/python/lzy/startup.py " + fileName + " " + fileOutName
    //    );
    //
    //    //Act
    //    servant.createChannel(channelName);
    //    servant.createSlot(localFileName, channelName, Utils.outFileSot());
    //    servant.createChannel(channelOutName);
    //    servant.createSlot(localFileOutName, channelOutName, Utils.inFileSot());
    //
    //    ForkJoinPool.commonPool()
    //        .execute(() -> servant.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
    //    servant.publish(python_io_file_lzy.getName(), python_io_file_lzy);
    //    final ExecutionResult[] result1 = new ExecutionResult[1];
    //    ForkJoinPool.commonPool()
    //        .execute(() -> result1[0] = servant.execute("bash", "-c", "cat " + localFileOutName));
    //    final ExecutionResult result = servant.run(
    //        python_io_file_lzy.getName(),
    //        "",
    //        Map.of(
    //            fileName.substring(LZY_MOUNT.length()), channelName,
    //            fileOutName.substring(LZY_MOUNT.length()), channelOutName
    //        )
    //    );
    //
    //    //Assert
    //    Assert.assertEquals(fileContent + "\n", result1[0].stdout());
    //    Assert.assertEquals(0, result.exitCode());
    //}

    //@Test
    //public void testDiamondNumbers() {
    //    final String generated0 = "generated_0";
    //    final String generated1 = "generated_1";
    //    final String incremented = "incremented";
    //    final String multiplied = "multiplied";
    //    final String sum = "sum";
    //
    //    final FileIOOperation generator = new FileIOOperation(
    //        "gen",
    //        Collections.emptyList(),
    //        List.of(), // generated0, generated1),
    //        "gen"
    //    );
    //
    //    final FileIOOperation incrementor = new FileIOOperation(
    //        List.of(generated0),
    //        List.of(incremented),
    //        "inc"
    //    );
    //
    //    final FileIOOperation multiplier = new FileIOOperation(
    //        List.of(generated1),
    //        List.of(multiplied),
    //        "mul"
    //    );
    //
    //    final FileIOOperation summer = new FileIOOperation(
    //        List.of(incremented, multiplied),
    //        List.of(sum),
    //        "add"
    //    );
    //
    //    final ExecutionResult run = publishAndRun(generator, "");
    //    run(incrementor);
    //    run(multiplier);
    //    run(summer);
    //}
}
