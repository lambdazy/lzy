package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

public class SnapshotTest extends LzyBaseTest {
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

    @After
    public void tearDown() {
        super.tearDown();
    }

    private String createSnapshot() throws ParseException {
        final String spIdJson = terminal.createSnapshot();
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
        Long fromDateLocalTimezone, Long toDateLocalTimezone) throws InvalidProtocolBufferException {
        String whiteboardsJson = terminal.whiteboards(namespace, tags, fromDateLocalTimezone, toDateLocalTimezone);
        LzyWhiteboard.WhiteboardsResponse.Builder builder = LzyWhiteboard.WhiteboardsResponse.newBuilder();
        JsonFormat.parser().merge(whiteboardsJson, builder);
        return builder.build().getWhiteboardsList();
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
            "/tmp/lzy/sbin/cat " + fileName + " > " + fileOutName
        );

        //Act
        terminal.createChannel(channelName);
        terminal.createSlot(localFileName, channelName, Utils.outFileSot());
        terminal.createChannel(channelOutName);
        terminal.createSlot(localFileOutName, channelOutName, Utils.inFileSot());

        ForkJoinPool.commonPool()
            .execute(() -> terminal.execute("bash", "-c", "echo " + fileContent + " > " + localFileName));
        terminal.publish(cat_to_file.getName(), cat_to_file);
        final LzyTerminalTestContext.Terminal.ExecutionResult[] result1 =
            new LzyTerminalTestContext.Terminal.ExecutionResult[1];
        ForkJoinPool.commonPool()
            .execute(() -> result1[0] = terminal.execute("bash", "-c", "/tmp/lzy/sbin/cat " + localFileOutName));
        final String spId = createSnapshot();
        Assert.assertNotNull(spId);

        final String firstTag = "firstTag";
        final String secondTag = "secondTag";
        final String namespace = "namespace";

        final String wbId =
            createWhiteboard(spId, List.of(localFileName, localFileOutName), List.of(firstTag, secondTag), namespace);
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

        terminal.link(wbId, localFileName, spId + "/" + firstEntryId);
        terminal.link(wbId, localFileOutName, spId + "/" + secondEntryId);

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

        if (localFileName.equals(fieldsList.get(0).getFieldName())) {
            Assert.assertEquals(Collections.emptyList(), fieldsList.get(0).getDependentFieldNamesList());
            Assert.assertEquals(List.of(localFileName), fieldsList.get(1).getDependentFieldNamesList());
        } else {
            Assert.assertEquals(Collections.emptyList(), fieldsList.get(1).getDependentFieldNamesList());
            Assert.assertEquals(List.of(localFileName), fieldsList.get(0).getDependentFieldNamesList());
        }
    }

    @Test
    public void testWhiteboardsResolving() throws ParseException, InvalidProtocolBufferException {
        final String spIdFirst = createSnapshot();
        Assert.assertNotNull(spIdFirst);

        final String spIdSecond = createSnapshot();
        Assert.assertNotNull(spIdSecond);

        final String firstTag = "firstTag";
        final String secondTag = "secondTag";
        final String thirdTag = "thirdTag";
        final String firstNamespace = "firstNamespace";
        final String secondNamespace = "secondNamespace";

        OffsetDateTime localDateTime = OffsetDateTime.now();
        Long timestamp = localDateTime.toInstant().getEpochSecond();
        localDateTime = localDateTime.plusDays(1);
        Long timestampNextDay = localDateTime.toInstant().getEpochSecond();

        final String wbIdFirst = createWhiteboard(
            spIdFirst, List.of("fileNameX", "fileNameY"), List.of(firstTag, secondTag), firstNamespace
        );
        Assert.assertNotNull(wbIdFirst);

        final String wbIdSecond = createWhiteboard(
            spIdFirst, List.of("fileNameZ", "fileNameW"), List.of(firstTag, secondTag, thirdTag), secondNamespace);
        Assert.assertNotNull(wbIdSecond);

        final String wbIdThird = createWhiteboard(
            spIdSecond, List.of("fileNameA", "fileNameB"), List.of(secondTag, thirdTag), firstNamespace);
        Assert.assertNotNull(wbIdThird);

        final String wbIdFourth = createWhiteboard(
            spIdSecond, List.of("fileNameC"), List.of(thirdTag), firstNamespace);
        Assert.assertNotNull(wbIdFourth);

        final String wbIdFifth = createWhiteboard(
            spIdSecond, List.of("fileNameD"), List.of(firstTag, thirdTag), secondNamespace);
        Assert.assertNotNull(wbIdFifth);

        List<LzyWhiteboard.Whiteboard> list = getWhiteboardsList(
            firstNamespace, List.of(secondTag), timestamp, timestampNextDay);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(
            list.stream()
                .map(LzyWhiteboard.Whiteboard::getId)
                .collect(Collectors.toList())
                .containsAll(List.of(wbIdFirst, wbIdThird))
        );

        list = getWhiteboardsList(firstNamespace, Collections.emptyList(), timestamp, timestampNextDay);
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(
            list.stream()
                .map(LzyWhiteboard.Whiteboard::getId)
                .collect(Collectors.toList())
                .containsAll(List.of(wbIdFirst, wbIdThird, wbIdFourth))
        );

        list = getWhiteboardsList(secondNamespace, List.of(firstTag, thirdTag), timestamp, timestampNextDay);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(
            list.stream()
                .map(LzyWhiteboard.Whiteboard::getId)
                .collect(Collectors.toList())
                .containsAll(List.of(wbIdSecond, wbIdFifth))
        );

        // intentionally setting the interval to be in the future
        timestamp = timestampNextDay;

        list = getWhiteboardsList(secondNamespace, Collections.emptyList(), timestamp, timestampNextDay);
        Assert.assertEquals(0, list.size());
    }
}
