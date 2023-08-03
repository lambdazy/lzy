package ai.lzy.test;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.test.context.LzyContextTests;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.common.*;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.graph.GraphExecutorApi.GraphStatusRequest;
import ai.lzy.worker.WorkerApiImpl;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.protobuf.Duration;
import jakarta.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static ai.lzy.v1.common.LMS.Slot.Direction.INPUT;
import static ai.lzy.v1.common.LMS.Slot.Direction.OUTPUT;

public class SchedulerTest extends LzyContextTests {
    static final Logger LOG = LogManager.getLogger(SchedulerTest.class);

    private AmazonS3 s3Client;

    @Before
    public void setUpS3() {
        s3Client = AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:" + s3Port, "us-west-1"))
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

        s3Client.createBucket("scheduler-test-bucket");
    }

    @Test(timeout = 240_000)
    public void testGE() throws Exception {
        WorkerApiImpl.TEST_ENV = true;

        var sid = allocatorGrpcClient
            .createSession(VmAllocatorApi.CreateSessionRequest.newBuilder()
                .setOwner("user")
                .setDescription("")
                .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder()
                    .setIdleTimeout(Duration.newBuilder()
                        .setSeconds(1000)
                        .build())
                    .build())
                .build())
            .getResponse()
            .unpack(VmAllocatorApi.CreateSessionResponse.class)
            .getSessionId();

        var cacheLimits = allocatorContext().getBean(ServiceConfig.CacheLimits.class);
        cacheLimits.setUserLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionPoolLimit(null);
        cacheLimits.setAnySessionPoolLimit(Integer.MAX_VALUE);

        final var ch1 = buildChannel("1");
        final var ch2 = buildChannel("2");
        final var ch3 = buildChannel("3");
        final var ch4 = buildChannel("4");

        final var t1 = buildTask("1", "echo 41 > $LZY_MOUNT/o1 && echo 42 > $LZY_MOUNT/o2",
            List.of(), List.of("/o1", "/o2"), Map.of("/o1", ch1, "/o2", ch2));

        final var t2 = buildTask("2", "cat $LZY_MOUNT/i1 > $LZY_MOUNT/o3", List.of("/i1"),
            List.of("/o3"), Map.of("/i1", ch1, "/o3", ch3));

        final var t3 = buildTask("3", "cat $LZY_MOUNT/i2 > $LZY_MOUNT/o4", List.of("/i2"),
            List.of("/o4"), Map.of("/i2", ch2, "/o4", ch4));

        final var t4 = buildTask("4", "cat $LZY_MOUNT/i3 >> /tmp/res.txt && cat $LZY_MOUNT/i4 >> /tmp/res.txt",
            List.of("/i3", "/i4"), List.of(), Map.of("/i3", ch3, "/i4", ch4));

        final var g1 = graphsGrpcClient.execute(GraphExecuteRequest.newBuilder()
            .setWorkflowId("wf_id")
            .setWorkflowName("wf")
            .setUserId("Semjon.Semjonych")
            .setAllocatorSessionId(sid)
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch1)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch2)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch3)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addChannels(ChannelDesc.newBuilder()
                .setId(ch4)
                .setDirect(ChannelDesc.DirectChannel.newBuilder().build())
                .build())
            .addAllTasks(List.of(t1, t2, t3, t4))
            .build()).getStatus().getGraphId();

        GraphExecutor.GraphExecutionStatus status;

        do {
            //noinspection BusyWait
            Thread.sleep(1000);
            status = graphsGrpcClient.status(GraphStatusRequest.newBuilder()
                .setGraphId(g1)
                .setWorkflowId("wf_id")
                .build()).getStatus();

            LOG.info("Exec status: {}", JsonUtils.printRequest(status));

        } while (!status.hasCompleted() && !status.hasFailed());

        Assert.assertTrue(status.hasCompleted());
    }

    @Nonnull
    private String buildChannel(String channelName) {
        final var response = privateChannelsGrpcClient.getOrCreate(
            LCMPS.GetOrCreateRequest.newBuilder()
                .setExecutionId("wf_id")
                .setUserId("Semjon.Semjonych")
                .setWorkflowName("wf")
                .setConsumer(LC.PeerDescription.StoragePeer.newBuilder()
                    .setStorageUri("s3://scheduler-test-bucket/" + channelName)
                    .setS3(LMST.S3Credentials.newBuilder()
                        .setEndpoint("http://localhost:" + s3Port)
                        .build())
                    .build())
                .build());
        return response.getChannelId();
    }

    private GraphExecutor.TaskDesc buildTask(String id, String command, List<String> inputs,
                                             List<String> outputs, Map<String, String> bindings)
    {
        final var op = LMO.Operation.newBuilder()
            .setEnv(
                LME.EnvSpec.newBuilder()
                    .setProcessEnv(LME.ProcessEnv.newBuilder().build())
                    .build()
            )
            .setRequirements(LMO.Requirements.newBuilder()
                .setPoolLabel("s")
                .setZone("A")
                .build())
            .setCommand(command)
            .addAllSlots(Stream.concat(
                inputs.stream().map(t -> buildSlot(t, INPUT)),
                outputs.stream().map(t -> buildSlot(t, OUTPUT))
            ).toList())
            .build();

        return GraphExecutor.TaskDesc.newBuilder()
            .setOperation(op)
            .setId(id)
            .addAllSlotAssignments(
                bindings.entrySet()
                    .stream()
                    .map(e -> GraphExecutor.SlotToChannelAssignment.newBuilder()
                        .setSlotName(e.getKey())
                        .setChannelId(e.getValue())
                        .build()
                    )
                    .toList()
            )
            .build();
    }

    public static LMS.Slot buildSlot(String name, LMS.Slot.Direction direction) {
        return LMS.Slot.newBuilder()
            .setContentType(LMD.DataScheme.newBuilder()
                .setSchemeFormat("plain")
                .setDataFormat("text")
                .build())
            .setMedia(LMS.Slot.Media.FILE)
            .setName(name)
            .setDirection(direction)
            .build();
    }
}
