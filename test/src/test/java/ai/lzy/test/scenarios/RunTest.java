package ai.lzy.test.scenarios;

import ai.lzy.test.LzyTerminalTestContext;
import ai.lzy.test.impl.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ai.lzy.fs.commands.BuiltinCommandHolder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class RunTest extends LocalScenario {
    @Before
    public void setUp() {
        super.setUp();
        startTerminalWithDefaultConfig();
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
        terminal.publish(echo42);
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(echo42.name(), "", Map.of());

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
            "sleep 20m; echo 42"
        );

        //Act
        terminal.publish(echo42);
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(echo42.name(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    public void testReadWrite() throws ExecutionException, InterruptedException {
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

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSlot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSlot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file);
        final CompletableFuture<LzyTerminalTestContext.Terminal.ExecutionResult> result = new CompletableFuture<>();

        ForkJoinPool.commonPool()
            .execute(() -> result.complete(
                terminal.run(
                    cat_to_file.name(),
                    "",
                    Map.of(
                        fileName.substring("/tmp/lzy1".length()), channelName,
                        fileOutName.substring("/tmp/lzy1".length()), channelOutName
                    )
                )
            ));

        final LzyTerminalTestContext.Terminal.ExecutionResult
            result1 = terminal.executeLzyCommand(BuiltinCommandHolder.cat, localFileOutName);

        //Assert
        Assert.assertEquals(0, result.get().exitCode());
        Assert.assertEquals(fileContent + "\n", result1.stdout());

        terminal.destroyChannel(channelName);
        terminal.destroyChannel(channelOutName);
    }
}
