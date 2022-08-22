package ai.lzy.servant.portal;

import ai.lzy.v1.LzyPortalApi;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

import static ai.lzy.test.GrpcUtils.*;

public class IncorrectCasesPortalTest extends PortalTest {
    @Test
    public void testSnapshotOnPortalWithNonActiveS3() throws Exception {
        stopS3();

        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channel for input portal slot
        createChannel("channel_1");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // configure portal to snapshot `channel-1` data on non-active S3
        String errorMessage = server.openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .build());

        Assert.assertTrue(errorMessage.contains("Unable to execute HTTP request"));
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }

    @Test
    public void openOutputSlotBeforeInputSlot() throws Exception {
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channel for portal output slot
        createChannel("channel_1");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // open portal output slot before input one was opened, there must be an error here
        String errorMessage = server.openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeOutputFileSlot("/slot_2"))
                .setChannelId("channel_1"))
            .build());

        Assert.assertEquals("Snapshot with id 'snapshot_1-lzy-bucket-http:localhost:8001' not found", errorMessage);
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }

    @Test
    public void makeSnapshotOfSlotThatAlreadyWasStored() throws Exception {
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channels for input portal slots
        createChannel("channel_1");
        createChannel("channel_2");

        // configure portal to snapshot `channel-1` data on S3
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .build());

        // just for logs
        Thread.sleep(Duration.ofSeconds(1).toMillis());
        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // snapshot portal_slot_1 one more time, there must be an error here
        String errorMessage = server.openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_2", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_2")
                .build())
            .build());

        Assert.assertEquals("Slot '/portal_slot_1' already associated with snapshot "
            + "'snapshot_1-lzy-bucket-http:localhost:8001'", errorMessage);
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }

    @Test
    public void makeSnapshotWithAlreadyUsedSnapshotId() throws Exception {
        startPortal();

        var portalStdout = readPortalSlot("portal:stdout");
        var portalStderr = readPortalSlot("portal:stderr");

        System.out.println("\n----- PREPARE PORTAL FOR SCENARIO -----------------------------------------\n");

        // create channels for scenario
        createChannel("channel_1");
        createChannel("channel_2");

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .build());

        // configure portal to snapshot `channel-2` data with same snapshot id, there must be an error here
        String errorMessage = server.openPortalSlotsWithFail(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_2"))
                .setChannelId("channel_2")
                .build())
            .build());

        Assert.assertEquals("Snapshot with id 'snapshot_1-lzy-bucket-http:localhost:8001' "
            + "already associated with data", errorMessage);
        Assert.assertTrue(portalStdout.isEmpty());
        Assert.assertTrue(portalStderr.isEmpty());

        // clean up
        System.out.println("-- cleanup scenario --");
        destroyChannel("channel_1");
        destroyChannel("channel_2");
        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }

    @Test
    public void readSnapshotOutputSlotBeforeInputOneWriteIt() throws Exception {
        startPortal();

        System.out.println("\n----- PREPARE PORTAL FOR TASK 1 -----------------------------------------\n");

        // create channels for task_1
        createChannel("channel_1");
        createChannel("task_1:stdout");
        createChannel("task_1:stderr");

        // configure portal to snapshot `channel-1` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeInputFileSlot("/portal_slot_1"))
                .setChannelId("channel_1")
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stdout"))
                .setChannelId("task_1:stdout")
                .setStdout(makeStdoutStorage("task_1"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_1:stderr"))
                .setChannelId("task_1:stderr")
                .setStderr(makeStderrStorage("task_1"))
                .build())
            .build());

        // create channels for task_2
        createChannel("channel_2");
        createChannel("task_2:stdout");
        createChannel("task_2:stderr");

        // configure portal to read snapshot `channel-2` data
        server.openPortalSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSnapshot(makeAmazonSnapshot("snapshot_1", BUCKET_NAME, S3_ADDRESS))
                .setSlot(makeOutputFileSlot("/portal_slot_2"))
                .setChannelId("channel_2")
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_2:stdout"))
                .setChannelId("task_2:stdout")
                .setStdout(makeStdoutStorage("task_2"))
                .build())
            .addSlots(LzyPortalApi.PortalSlotDesc.newBuilder()
                .setSlot(makeInputFileSlot("/portal_task_2:stderr"))
                .setChannelId("task_2:stderr")
                .setStderr(makeStderrStorage("task_2"))
                .build())
            .build());

        System.out.println("\n----- RUN SCENARIO -----------------------------------------\n");

        var snapshotData = readPortalSlot("channel_2");
        Object obj = snapshotData.take();

        Assert.assertSame(obj.getClass(), StatusRuntimeException.class);

        var expected = "Input slot of this snapshot is not already connected";
        var actual = ((StatusRuntimeException) obj).getStatus().getDescription();

        Assert.assertEquals(expected, actual);

        server.waitPortalCompleted();

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

        destroyChannel("portal:stdout");
        destroyChannel("portal:stderr");
    }
}
