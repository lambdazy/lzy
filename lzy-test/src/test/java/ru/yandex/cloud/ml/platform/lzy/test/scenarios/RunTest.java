package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext.Servant.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServantDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("CommentedOutCode")
public class RunTest {
    private static final int DEFAULT_SERVANT_INIT_TIMEOUT_SEC = 30;
    private static final String LZY_MOUNT = "/tmp/lzy";

    private LzyServerTestContext server;
    private LzyServantTestContext servantContext;
    private LzyServantTestContext.Servant servant;

    @Before
    public void setUp() {
        server = new LzyServerProcessesContext();
        servantContext = new LzyServantDockerContext();
        servant = servantContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            9999,
            server.host(servantContext.inDocker()),
            server.port()
        );
        servant.waitForStatus(
            ServantStatus.EXECUTING,
            DEFAULT_SERVANT_INIT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    @After
    public void tearDown() {
        servantContext.close();
        server.close();
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
        servant.publish(echo42.getName(), echo42);
        final ExecutionResult result = servant.run(echo42.getName(), "", Map.of());

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
        servant.createChannel(channelName);
        servant.createSlot(localFileName, channelName, Utils.outFileSot());
        ForkJoinPool.commonPool()
            .execute(() -> servant.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        servant.publish(cat.getName(), cat);
        final ExecutionResult result = servant.run(
            cat.getName(),
            "",
            Map.of(fileName.substring(LZY_MOUNT.length()), channelName)
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result.stdout());
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
        servant.createChannel(channelOutName);
        servant.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        servant.publish(echo_lzy.getName(), echo_lzy);
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = servant.execute("bash", "-c", "cat " + localFileOutName));
        final ExecutionResult result = servant.run(
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
        servant.createChannel(channelName);
        servant.createSlot(localFileName, channelName, Utils.outFileSot());
        servant.createChannel(channelOutName);
        servant.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        ForkJoinPool.commonPool()
            .execute(() -> servant.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        servant.publish(cat_to_file.getName(), cat_to_file);
        final ExecutionResult[] result1 = new ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = servant.execute("bash", "-c", "cat " + localFileOutName));
        final ExecutionResult result = servant.run(
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
