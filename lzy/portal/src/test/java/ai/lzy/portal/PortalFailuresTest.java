package ai.lzy.portal;

import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;


public class PortalFailuresTest extends PortalTestBase {
    @Test
    public void testSnapshotOnPortalWithNonActiveS3() {
        stopS3();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

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
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
    }

    @Test
    public void openOutputSlotBeforeInputSlot() {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

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
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
    }

    @Test
    public void makeSnapshotOfSlotThatAlreadyWasStored() throws Exception {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

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
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
    }

    @Test
    public void makeSnapshotWithAlreadyUsedSnapshotId() {
        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

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
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
    }

    @Test
    @Ignore
    public void readSnapshotOutputSlotBeforeInputOneWriteIt() throws Exception {
        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        String channelId1 = createChannel("channel_1");
        String stdoutChannelId1 = createChannel("task_1:stdout");
        String stderrChannelId1 = createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_slot_1"))
                .setChannelId(channelId1)
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_task_1:stdout"))
                .setChannelId(stdoutChannelId1)
                .setStdout(GrpcUtils.makeStdoutStorage("task_1"))
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_task_1:stderr"))
                .setChannelId(stderrChannelId1)
                .setStderr(GrpcUtils.makeStderrStorage("task_1"))
                .build())
            .build());

        // create channels for task_2
        String channelId2 = createChannel("channel_2");
        String stdoutChannelId2 = createChannel("task_2:stdout");
        String stderrChannelId2 = createChannel("task_2:stderr");

        // configure portal to read snapshot `channel-2` data
        openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSnapshot(GrpcUtils.makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(GrpcUtils.makeOutputFileSlot("/portal_slot_2"))
                .setChannelId(channelId2)
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_task_2:stdout"))
                .setChannelId(stdoutChannelId2)
                .setStdout(GrpcUtils.makeStdoutStorage("task_2"))
                .build())
            .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                .setSlot(GrpcUtils.makeInputFileSlot("/portal_task_2:stderr"))
                .setChannelId(stderrChannelId2)
                .setStderr(GrpcUtils.makeStderrStorage("task_2"))
                .build())
            .build());

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        var snapshotData = readPortalSlot("channel_2");
        Object obj = snapshotData.take();

        Assert.assertSame(obj.getClass(), StatusRuntimeException.class);

        var expected = "Input slot of this snapshot is not already connected";
        var actual = ((StatusRuntimeException) obj).getStatus().getDescription();

        Assert.assertEquals(expected, actual);

        waitPortalCompleted(9);

        Assert.assertTrue(snapshotData.isEmpty());

        // task_1 clean up
        System.out.println("-- cleanup task1 scenario --");
        destroyChannel("channel_1");
        destroyChannel("task_1:stdout");
        destroyChannel("task_1:stderr");

        // task_2 clean up
        System.out.println("-- cleanup task2 scenario --");
        destroyChannel("channel_2");
        destroyChannel("task_2:stdout");
        destroyChannel("task_2:stderr");
    }
}
