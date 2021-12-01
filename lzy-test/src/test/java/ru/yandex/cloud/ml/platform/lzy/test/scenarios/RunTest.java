package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.jose4j.json.internal.json_simple.JSONArray;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.jose4j.json.internal.json_simple.parser.JSONParser;
import org.jose4j.json.internal.json_simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import io.findify.s3mock.S3Mock;

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
        Assert.assertEquals(fileContent + "\n", terminal.execute("bash", "-c", "cat " + localFileName).stdout());

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

    @Test
    public void testTaskPersistent() throws IOException, ParseException {
        final S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        api.start();

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
        final UUID wbId = UUID.randomUUID();
        final String taskName = "taskName";
        final String arguments = "--persistent " + wbId + " -n " + taskName;
        final ExecutionResult result = terminal.run(
                cat_to_file.getName(),
                arguments,
                Map.of(
                        fileName.substring(LZY_MOUNT.length()), channelName,
                        fileOutName.substring(LZY_MOUNT.length()), channelOutName
                ),
                List.of("a", "b", "c")
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());

        final AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-west-2"))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        List<S3ObjectSummary> objects = client.listObjects("lzy-bucket").getObjectSummaries();
        Assert.assertEquals(2, objects.size());

        for (var obj : objects) {
            String key = obj.getKey();
            String content = IOUtils.toString(
                    client.getObject(new GetObjectRequest("lzy-bucket", key))
                            .getObjectContent(),
                    StandardCharsets.UTF_8
            );
            Assert.assertEquals(fileContent + "\n", content);
        }

        String whiteboard = terminal.getWhiteboard(wbId.toString());
        JSONObject jsonObject = (JSONObject) (new JSONParser()).parse(whiteboard);

        JSONArray storageBindings = (JSONArray) jsonObject.get("storageBindings");
        Assert.assertEquals(1, storageBindings.size());
        JSONObject storageBinding = (JSONObject) storageBindings.get(0);
        Assert.assertEquals(storageBinding.get("fieldName"), taskName);
        Assert.assertTrue(storageBinding.get("storageUri").toString().length() > 0);

        JSONArray relations = (JSONArray) jsonObject.get("relations");
        Assert.assertEquals(1, relations.size());
        JSONObject relation = (JSONObject) relations.get(0);
        Assert.assertEquals(relation.get("fieldName"), taskName);
        JSONArray dependencies = (JSONArray) relation.get("dependencies");
        Assert.assertEquals(3, dependencies.size());
        Assert.assertTrue(dependencies.contains("a") && dependencies.contains("b") && dependencies.contains("c"));

        api.shutdown();
    }
}
