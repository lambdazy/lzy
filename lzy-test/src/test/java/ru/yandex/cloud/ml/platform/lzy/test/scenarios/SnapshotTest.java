package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.IOUtils;
import org.jose4j.json.internal.json_simple.JSONObject;
import org.jose4j.json.internal.json_simple.parser.JSONParser;
import org.jose4j.json.internal.json_simple.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.LzyFsServer;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.Utils;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class SnapshotTest extends LzyBaseTest {
    private LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        super.setUp();
        terminal = terminalContext.startTerminalAtPathAndPort(
            LZY_MOUNT,
            9999,
            LzyFsServer.DEFAULT_PORT,
            kharonContext.serverAddress(terminalContext.inDocker())
        );
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    private String createSnapshot() throws ParseException {
        final String spIdJson = terminal.createSnapshot("some-workflow");
        JSONObject spIdObject = (JSONObject) (new JSONParser()).parse(spIdJson);
        return (String) spIdObject.get("snapshotId");
    }

    private String createWhiteboard(String spId, List<String> fileNames, List<String> tags, String namespace)
        throws ParseException {
        String wbIdJson = terminal.createWhiteboard(spId, fileNames, tags, namespace);
        JSONObject wbIdObject = (JSONObject) (new JSONParser()).parse(wbIdJson);
        return (String) wbIdObject.get("id");
    }

    private List<LzyWhiteboard.Whiteboard> getWhiteboardsList(String namespace, List<String> tags,
                                                              Long fromDateLocalTimezone, Long toDateLocalTimezone)
        throws InvalidProtocolBufferException {
        String whiteboardsJson = terminal.whiteboards(namespace, tags, fromDateLocalTimezone, toDateLocalTimezone);
        LzyWhiteboard.WhiteboardsResponse.Builder builder = LzyWhiteboard.WhiteboardsResponse.newBuilder();
        JsonFormat.parser().merge(whiteboardsJson, builder);
        return builder.build().getWhiteboardsList();
    }

    @Test
    public void testTaskPersistent() throws IOException, ParseException, ExecutionException, InterruptedException {
        //Arrange
        final String channelEntryId = "firstEntryId";
        final String channelOutEntryId = "secondEntryId";

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
            "/tmp/lzy1/sbin/cat " + fileName + " > " + fileOutName,
            false
        );

        //Act
        final String spId = createSnapshot();

        terminal.createChannel(channelName, spId, spId + "/" + channelEntryId);
        terminal.createSlot(localFileName, channelName, Utils.outFileSlot());
        terminal.createChannel(channelOutName, spId, spId + "/" + channelOutEntryId);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSlot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file.getName(), cat_to_file);

        final String firstTag = "firstTag";
        final String secondTag = "secondTag";
        final String namespace = "namespace";

        final String wbId =
            createWhiteboard(spId, List.of(localFileName, localFileOutName), List.of(firstTag, secondTag), namespace);
        Assert.assertNotNull(wbId);

        final CompletableFuture<LzyTerminalTestContext.Terminal.ExecutionResult> result = new CompletableFuture<>();

        ForkJoinPool.commonPool()
            .execute(() -> result.complete(
                terminal.run(
                    cat_to_file.getName(),
                    "",
                    Map.of(
                        fileName.substring("/tmp/lzy1".length()), channelName,
                        fileOutName.substring("/tmp/lzy1".length()), channelOutName
                    )
                )
            ));

        final LzyTerminalTestContext.Terminal.ExecutionResult result1 = terminal.execute("bash", "-c",
            "/tmp/lzy/sbin/cat " + localFileOutName);

        //Assert
        Assert.assertEquals(0, result.get().exitCode());
        Assert.assertEquals(fileContent + "\n", result1.stdout());

        terminal.destroyChannel(channelName);
        terminal.destroyChannel(channelOutName);

        final AmazonS3 client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + S3_PORT, "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        List<Bucket> bucketList = client.listBuckets();
        Assert.assertEquals(1, bucketList.size());
        String bucketName = bucketList.get(0).getName();
        List<S3ObjectSummary> objects = client.listObjects(bucketName).getObjectSummaries();
        Assert.assertEquals(2, objects.size());

        for (var obj : objects) {
            String key = obj.getKey();
            String content = IOUtils.toString(
                client.getObject(new GetObjectRequest(bucketName, key))
                    .getObjectContent(),
                StandardCharsets.UTF_8
            );
            Assert.assertEquals(fileContent + "\n", content);
        }

        terminal.link(wbId, localFileName, spId + "/" + channelEntryId);
        terminal.link(wbId, localFileOutName, spId + "/" + channelOutEntryId);

        terminal.finalizeSnapshot(spId);
        String whiteboard = terminal.getWhiteboard(wbId);

        LzyWhiteboard.Whiteboard.Builder builder = LzyWhiteboard.Whiteboard.newBuilder();
        JsonFormat.parser().merge(whiteboard, builder);
        LzyWhiteboard.Whiteboard wb = builder.build();
        final List<LzyWhiteboard.WhiteboardField> fieldsList = wb.getFieldsList();

        Assert.assertEquals(spId, wb.getSnapshot().getSnapshotId());
        Assert.assertEquals("test-user:" + namespace, wb.getNamespace());
        Assert.assertEquals(2, fieldsList.size());
        Assert.assertEquals(LzyWhiteboard.WhiteboardStatus.COMPLETED, wb.getStatus());

        Assert.assertTrue(
            (localFileName.equals(fieldsList.get(0).getFieldName()) &&
                localFileOutName.equals(fieldsList.get(1).getFieldName())) ||
                (localFileName.equals(fieldsList.get(1).getFieldName()) &&
                    localFileOutName.equals(fieldsList.get(0).getFieldName()))
        );

        Assert.assertTrue(fieldsList.get(0).getStorageUri().length() > 0);
        Assert.assertTrue(fieldsList.get(1).getStorageUri().length() > 0);

        final List<String> tagsList = wb.getTagsList();
        Assert.assertEquals(2, tagsList.size());
        Assert.assertTrue(
            (firstTag.equals(tagsList.get(0)) && secondTag.equals(tagsList.get(1))) ||
                (firstTag.equals(tagsList.get(1)) && secondTag.equals(tagsList.get(0)))
        );
    }
}
