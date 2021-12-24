package ru.yandex.cloud.ml.platform.lzy.test.scenarios.processes;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
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
import ru.yandex.cloud.ml.platform.lzy.test.scenarios.FileIOOperation;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

public class SnapshotTest extends LzyBaseProcessTest {

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

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testTaskPersistent() throws IOException, ParseException {
        //Arrange
        final String fileContent = "fileContent";
        final String fileName =  "/kek/some_file.txt";
        final String localFileName = defaultLzyMount() + "/lol/some_file.txt";
        final String channelName = "channel1";

        final String fileOutName = "/kek/some_file_out.txt";
        final String localFileOutName = defaultLzyMount() + "/lol/some_file_out.txt";
        final String channelOutName = "channel2";

        final FileIOOperation cat_to_file = new FileIOOperation(
            "cat_to_file_lzy",
            List.of(fileName),
            List.of(fileOutName),
            "$(echo $LZY_MOUNT)/sbin/cat $(echo $LZY_MOUNT)/" + fileName + " > $(echo $LZY_MOUNT)/" + fileOutName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c",
                "echo " + fileContent + " > " + localFileName));
        final LzyTerminalTestContext.Terminal.ExecutionResult[] result1 = new LzyTerminalTestContext.Terminal.ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = terminal.execute("bash", "-c",
                "$(echo $LZY_MOUNT)/sbin/cat " + localFileOutName));
        final String spIdJson = terminal.createSnapshot();
        JSONObject spIdObject = (JSONObject) (new JSONParser()).parse(spIdJson);
        final String spId = (String) spIdObject.get("snapshotId");
        Assert.assertNotNull(spId);

        String wbIdJson = terminal.createWhiteboard(spId, List.of(localFileName, localFileOutName));
        JSONObject wbIdObject = (JSONObject) (new JSONParser()).parse(wbIdJson);
        final String wbId = (String) wbIdObject.get("id");
        Assert.assertNotNull(wbId);

        final String firstEntryId = "firstEntryId";
        final String secondEntryId = "secondEntryId";
        final String stderrEntryId = "stderrEntryId";
        final String stdoutEntryId = "stdoutEntryId";
        final String stdinEntryId = "stdinEntryId";
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(
            cat_to_file,
            Map.of(
                fileName, channelName,
                fileOutName, channelOutName
            ),
            Map.of(
                fileName, spId + "/" + firstEntryId,
                fileOutName, spId + "/" + secondEntryId,
                "/dev/stderr", spId + "/" + stderrEntryId,
                "/dev/stdout", spId + "/" + stdoutEntryId,
                "/dev/stdin", spId + "/" + stdinEntryId
            ),
            ""
        );

        //Assert
        Assert.assertEquals(fileContent + "\n", result1[0].stdout());
        Assert.assertEquals(0, result.exitCode());

        final AmazonS3 client = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + s3Port(),
                    "us-west-2"))
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
                client.getObject(new GetObjectRequest(bucketName, key)).getObjectContent(),
                StandardCharsets.UTF_8
            );
            Assert.assertEquals(fileContent + "\n", content);
        }

        terminal.link(wbId, localFileName, spId + "/" + firstEntryId);
        terminal.link(wbId, localFileOutName, spId + "/" + secondEntryId);

        terminal.finalizeSnapshot(spId);
        String whiteboard = terminal.getWhiteboard(wbId);

        LzyWhiteboard.Whiteboard.Builder builder = LzyWhiteboard.Whiteboard.newBuilder();
        JsonFormat.parser().merge(whiteboard, builder);
        LzyWhiteboard.Whiteboard wb = builder.build();
        final List<LzyWhiteboard.WhiteboardField> fieldsList = wb.getFieldsList();

        Assert.assertEquals(spId, wb.getSnapshot().getSnapshotId());
        Assert.assertEquals(2, fieldsList.size());
        Assert.assertEquals(LzyWhiteboard.Whiteboard.WhiteboardStatus.COMPLETED, wb.getStatus());

        Assert.assertTrue(
            (localFileName.equals(fieldsList.get(0).getFieldName()) && localFileOutName.equals(
                fieldsList.get(1).getFieldName())) ||
                (localFileName.equals(fieldsList.get(1).getFieldName()) && localFileOutName.equals(
                    fieldsList.get(0).getFieldName()))
        );

        Assert.assertTrue(fieldsList.get(0).getStorageUri().length() > 0);
        Assert.assertTrue(fieldsList.get(1).getStorageUri().length() > 0);

        if (localFileName.equals(fieldsList.get(0).getFieldName())) {
            Assert.assertEquals(Collections.emptyList(),
                fieldsList.get(0).getDependentFieldNamesList());
            Assert.assertEquals(List.of(localFileName),
                fieldsList.get(1).getDependentFieldNamesList());
        } else {
            Assert.assertEquals(Collections.emptyList(),
                fieldsList.get(1).getDependentFieldNamesList());
            Assert.assertEquals(List.of(localFileName),
                fieldsList.get(0).getDependentFieldNamesList());
        }
    }
}
