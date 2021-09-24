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
            server.host(true),
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

    @SuppressWarnings("SameParameterValue")
    private ExecutionResult publishAndRun(FileIOOperation operation, String arguments) {
        servant.publish(operation.getName(), operation);
        final ExecutionResult result = servant.run(operation.getName(), arguments);

        System.out.print("\u001B[31m");
        System.out.println("EXECUTED COMMAND: " + operation.getName());
        System.out.print("\u001B[30m");
        System.out.println("Stdout:\n" + result.stdout());
        System.out.println("Stderr:\n" + result.stderr());
        System.out.println("ExitCode:\n" + result.exitCode());
        return result;
    }

    @Test
    public void testEcho42() {
        final FileIOOperation echo42 = new FileIOOperation(
            "echo42",
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42"
        );
        final ExecutionResult result = publishAndRun(echo42, "");
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    public void testInputLocalFileReturnStdout() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy/tmp/some_file.txt";
        final String channelName = "channel1";
        final FileIOOperation cat = new FileIOOperation(
            "cat_lzy",
            List.of(fileName.substring(LZY_MOUNT.length())),
            Collections.emptyList(),
            "cat " + fileName
        );

        //Act
        servant.createChannel(channelName);
        servant.createSlot(fileName, channelName, Utils.outFileSot());
        ForkJoinPool.commonPool()
            .execute(() -> servant.execute("bash", "-c", "echo " + fileContent + " > " + fileName));
        final ExecutionResult result = publishAndRun(cat, "");

        //Assert
        Assert.assertEquals(fileContent + "\n", result.stdout());
    }

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
