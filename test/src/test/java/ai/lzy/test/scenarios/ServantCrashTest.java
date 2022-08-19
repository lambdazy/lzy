package ai.lzy.test.scenarios;

import ai.lzy.test.LzyTerminalTestContext;
import ai.lzy.test.impl.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ai.lzy.model.ReturnCodes;
import ai.lzy.servant.env.EnvironmentFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public class ServantCrashTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        startTerminalWithDefaultConfig();
    }

    @Test
    public void testErrorDuringEnvPrepare() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy1/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/tmp/lzy1/kek/some_file_out.txt";
        final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat_to_file = new FileIOOperation(
                "cat_to_file_lzy",
                List.of(fileName.substring("/tmp/lzy1".length())),
                List.of(fileOutName.substring("/tmp/lzy1".length())),
                "/tmp/lzy1/sbin/cat " + fileName + " > " + fileOutName
        );

        final String channelId = terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelId, Utils.outFileSlot());
        final String channelOutId = terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutId, Utils.inFileSlot());
        ForkJoinPool.commonPool()
                .execute(() -> terminal.execute("echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file);

        //Act
        EnvironmentFactory.envForTests(() -> {
            throw new RuntimeException();
        });
        final LzyTerminalTestContext.Terminal.ExecutionResult run = terminal.run(
                cat_to_file.name(),
                "",
                Map.of(
                        fileName.substring("/tmp/lzy1".length()), channelId,
                        fileOutName.substring("/tmp/lzy1".length()), channelOutId
                )
        );

        //Assert
        Assert.assertEquals(ReturnCodes.INTERNAL_ERROR.getRc(), run.exitCode());

        terminal.destroyChannel(channelId);
        terminal.destroyChannel(channelOutId);
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testErrorDuringExecution() {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName = "/tmp/lzy1/kek/some_file.txt";
        final String localFileName = "/tmp/lzy/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/tmp/lzy1/kek/some_file_out.txt";
        final String localFileOutName = "/tmp/lzy/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat_to_file = new FileIOOperation(
                "cat_to_file_lzy",
                List.of(fileName.substring("/tmp/lzy1".length())),
                List.of(fileOutName.substring("/tmp/lzy1".length())),
                "/tmp/lzy1/sbin/cat " + fileName + " > " + fileOutName
        );
        final String channelId = terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelId, Utils.outFileSlot());
        final String channelOutId = terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutId, Utils.inFileSlot());
        ForkJoinPool.commonPool()
                .execute(() -> terminal.execute("echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file);

        //Act
        EnvironmentFactory.envForTests(() -> null);
        final LzyTerminalTestContext.Terminal.ExecutionResult run = terminal.run(
                cat_to_file.name(),
                "",
                Map.of(
                        fileName.substring("/tmp/lzy1".length()), channelId,
                        fileOutName.substring("/tmp/lzy1".length()), channelOutId
                )
        );

        //Assert
        Assert.assertEquals(ReturnCodes.INTERNAL_ERROR.getRc(), run.exitCode());

        terminal.destroyChannel(channelId);
        terminal.destroyChannel(channelOutId);
    }
}
