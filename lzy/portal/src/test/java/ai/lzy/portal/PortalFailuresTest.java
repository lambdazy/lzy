package ai.lzy.portal;

import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.List;


public class PortalFailuresTest extends PortalTestBase {
    @Test
    public void testSnapshotOnPortalWithNonActiveS3() throws InterruptedException {
        stopS3();

        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channel for input portal slot
        createChannel("channel_1");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // configure portal to snapshot `channel-1` data on non-active S3
        var status = openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .build());

        Assert.assertEquals(Status.Code.INTERNAL, status.getCode());
        assertStdLogs(stdlogs, List.of(), List.of());
        finishStdlogsReader.finish();

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
    }

    @Test
    public void openOutputSlotBeforeInputSlot() throws InterruptedException {
        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channel for portal output slot
        createChannel("channel_1");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // open portal output slot before input one was opened, there must be an error here
        var status = openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeOutputFileSlot("/slot_2"))
                .setChannelId("channel_1"))
            .build());

        Assert.assertEquals(Status.Code.NOT_FOUND, status.getCode());
        assertStdLogs(stdlogs, List.of(), List.of());
        finishStdlogsReader.finish();

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
    }

    @Test
    public void makeSnapshotOfSlotThatAlreadyWasStored() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channels for input portal slots
        String channelId1 = createChannel("channel_1");
        String channelId2 = createChannel("channel_2");

        // configure portal to snapshot `channel-1` data on S3
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId(channelId1)
                .build())
            .build());

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // snapshot portal_slot_1 one more time, there must be an error here
        var status = openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_2", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId(channelId2)
                .build())
            .build());

        Assert.assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertStdLogs(stdlogs, List.of(), List.of());
        finishStdlogsReader.finish();

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
    }

    @Test
    public void makeSnapshotWithAlreadyUsedSnapshotId() throws InterruptedException {
        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channels for scenario
        String channelId1 = createChannel("channel_1");
        String channelId2 = createChannel("channel_2");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // configure portal to snapshot `channel-1` data
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId(channelId1)
                .build())
            .build());

        // configure portal to snapshot `channel-2` data with same snapshot id, there must be an error here
        var status = openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_2"))
                .setChannelId(channelId2)
                .build())
            .build());

        Assert.assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertStdLogs(stdlogs, List.of(), List.of());
        finishStdlogsReader.finish();

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
    }

    @Test
    public void readSnapshotOutputSlotBeforeInputOneWriteIt() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        String channelId1 = createChannel("channel_1");

        // configure portal to snapshot `channel-1` data
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId(channelId1)
                .build())
            .build());

        // create channels for task_2
        String channelId2 = createChannel("channel_2");

        // configure portal to read snapshot `channel-2` data
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeOutputFileSlot("/portal_slot_2"))
                .setChannelId(channelId2)
                .build())
            .build());

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        var snapshotData = readPortalSlot("channel_2");
        Object obj = snapshotData.take();

        Assert.assertSame(obj.getClass(), StatusRuntimeException.class);

        var expected = "Input slot of this snapshot is not already connected";
        var actual = ((StatusRuntimeException) obj).getStatus().getDescription();

        Assert.assertEquals(expected, actual);

        waitPortalCompleted();

        Assert.assertTrue(snapshotData.isEmpty());
        assertStdLogs(stdlogs, List.of(), List.of());
        finishStdlogsReader.finish();

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");
    }
}
