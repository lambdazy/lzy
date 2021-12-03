package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.jose4j.json.internal.json_simple.JSONArray;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.jose4j.json.internal.json_simple.parser.JSONParser;
import org.jose4j.json.internal.json_simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class SnapshotTest extends LzyBaseTest {
    private LzyTerminalTestContext.Terminal terminal;
    private S3Mock api;

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

        api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        api.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        api.shutdown();
    }

    @Test
    public void testTaskPersistent() throws IOException, ParseException {
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
        final LzyTerminalTestContext.Terminal.ExecutionResult[] result1 = new LzyTerminalTestContext.Terminal.ExecutionResult[1];
        ForkJoinPool.commonPool()
                .execute(() -> result1[0] = terminal.execute("bash", "-c", "cat " + localFileOutName));
        final String spIdJson = terminal.createSnapshot();
        JSONObject spIdObject = (JSONObject) (new JSONParser()).parse(spIdJson);
        final String spId = (String) spIdObject.get("snapshotId");
        Assert.assertNotNull(spId);

        String wbIdJson = terminal.createWhiteboard(spId);
        JSONObject wbIdObject = (JSONObject) (new JSONParser()).parse(wbIdJson);
        final String wbId = (String) wbIdObject.get("wbId");
        Assert.assertNotNull(wbId);

        final String firstEntryId = "firstEntryId";
        final String secondEntryId = "secondEntryId";
        final String stderrEntryId = "stderrEntryId";
        final String stdoutEntryId = "stdoutEntryId";
        final String stdinEntryId = "stdinEntryId";
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(
                cat_to_file.getName(),
                "",
                Map.of(
                        fileName.substring(LZY_MOUNT.length()), channelName,
                        fileOutName.substring(LZY_MOUNT.length()), channelOutName
                ),
                Map.of(
                        fileName.substring(LZY_MOUNT.length()), spId + "/" + firstEntryId,
                        fileOutName.substring(LZY_MOUNT.length()), spId + "/" + secondEntryId,
                        "/dev/stderr", spId + "/" + stderrEntryId,
                        "/dev/stdout", spId + "/" + stdoutEntryId,
                        "/dev/stdin", spId + "/" + stdinEntryId
                )
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

        terminal.addLink(wbId, Map.of(
                localFileName, spId + "/" + firstEntryId,
                localFileOutName, spId + "/" + secondEntryId
        ));

        terminal.finalizeWhiteboard(wbId);
        String whiteboard = terminal.getWhiteboard(wbId);
        JSONObject jsonObject = (JSONObject) (new JSONParser()).parse(whiteboard);

        JSONArray storageBindings = (JSONArray) jsonObject.get("storageBindings");
        Assert.assertEquals(2, storageBindings.size());
        JSONObject storageBinding = (JSONObject) storageBindings.get(0);
        Assert.assertTrue(storageBinding.get("fieldName").equals(localFileOutName) ||
                storageBinding.get("fieldName").equals(localFileName));
        Assert.assertTrue(storageBinding.get("storageUri").toString().length() > 0);

        storageBinding = (JSONObject) storageBindings.get(1);
        Assert.assertTrue(storageBinding.get("fieldName").equals(localFileOutName) ||
                storageBinding.get("fieldName").equals(localFileName));
        Assert.assertTrue(storageBinding.get("storageUri").toString().length() > 0);

        JSONArray relations = (JSONArray) jsonObject.get("relations");
        Assert.assertEquals(1, relations.size());
        JSONObject relation = (JSONObject) relations.get(0);
        Assert.assertEquals(relation.get("fieldName"), localFileOutName);
        JSONArray dependencies = (JSONArray) relation.get("dependencies");
        Assert.assertEquals(1, dependencies.size());
        Assert.assertEquals(dependencies.get(0), localFileName);
    }
}
