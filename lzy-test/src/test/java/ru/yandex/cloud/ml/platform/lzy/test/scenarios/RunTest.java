package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServantTestContext.Servant.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServantDockerContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.LzyServerProcessesContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("CommentedOutCode")
public class RunTest {
    private static final int DEFAULT_SERVANT_INIT_TIMEOUT_SEC = 30;

    private LzyServerTestContext server;
    private LzyServantTestContext servantContext;
    private LzyServantTestContext.Servant servant;

    @Before
    public void setUp() {
        server = new LzyServerProcessesContext();
        servantContext = new LzyServantDockerContext();
        servant = servantContext.startTerminalAtPathAndPort(
            "/tmp/lzy",
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
    public void testSingleOp() {
        //Arrange
        final FileIOOperation generator = new FileIOOperation(
            Collections.emptyList(),
            List.of(), // generated0, generated1),
            "gen"
        );

        //Act
        final ExecutionResult run = publishAndRun(generator, "");

        //Assert
        Assert.assertEquals("42\n", run.stdout());
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
