package ai.lzy.channelmanager;

import ai.lzy.channelmanager.v2.debug.InjectedFailures;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMS.BindRequest;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ChannelManagerRestartTest extends ChannelManagerBaseApiTest {

    @Before
    public void before() throws IOException, InterruptedException {
        super.before();
    }

    @After
    public void after() throws InterruptedException {
        super.after();
    }

    @Test
    public void testBindReceiverRestart() {
        final String executionId = UUID.randomUUID().toString();

        var channel = prepareChannelOfShape(executionId, "channel0", 0, 1, 0, 0);

        InjectedFailures.setFailure(11, 1);
        InjectedFailures.setFailure(0, 1);
        InjectedFailures.setFailure(1, 1);
        InjectedFailures.setFailure(2, 1);
        InjectedFailures.setFailure(3, 1);

        var op = publicClient.bind(makeBindCommand(channel.getChannelId(), "inSlotW",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER));

        skipAndRestoreOperation(op.getId(), 5);

        awaitOperationResponse(op.getId());
    }

    @Test
    public void testBindSenderRestart() {
        final String executionId = UUID.randomUUID().toString();

        var channel = prepareChannelOfShape(executionId, "channel0", 0, 0, 0, 3);

        InjectedFailures.setFailure(11, 1);
        InjectedFailures.setFailure(0, 2);
        InjectedFailures.setFailure(1, 2);
        InjectedFailures.setFailure(2, 2);
        InjectedFailures.setFailure(3, 2);

        var op = publicClient.bind(makeBindCommand(channel.getChannelId(), "outSlotW",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER));

        skipAndRestoreOperation(op.getId(), 5);

        awaitOperationResponse(op.getId());
    }

    @Test
    public void testUnbindReceiverRestart() {
        final String executionId = UUID.randomUUID().toString();

        var channel = prepareChannelOfShape(executionId, "channel0", 0, 1, 0, 1);
        final String slotUri = channel.getReceivers().getWorkerSlots(0).getSlotUri();

        InjectedFailures.setFailure(12, 1);
        InjectedFailures.setFailure(4, 1);
        InjectedFailures.setFailure(5, 1);

        var op = publicClient.unbind(makeUnbindCommand(slotUri));

        skipAndRestoreOperation(op.getId(), 3);

        awaitOperationResponse(op.getId());
    }

    @Test
    public void testUnbindSenderRestart() {
        final String executionId = UUID.randomUUID().toString();

        var channel = prepareChannelOfShape(executionId, "channel0", 0, 1, 1, 2);
        final String slotUri = channel.getSenders().getWorkerSlot().getSlotUri();

        InjectedFailures.setFailure(12, 1);
        InjectedFailures.setFailure(8, 1);
        InjectedFailures.setFailure(4, 2);
        InjectedFailures.setFailure(5, 2);
        InjectedFailures.setFailure(6, 2);
        InjectedFailures.setFailure(7, 2);

        var op = publicClient.unbind(makeUnbindCommand(slotUri));

        skipAndRestoreOperation(op.getId(), 6);

        awaitOperationResponse(op.getId());
    }

    @Test
    public void testDestroyRestart() {
        final String executionId = UUID.randomUUID().toString();

        var channel = prepareChannelOfShape(executionId, "channel0", 1, 0, 0, 1);

        InjectedFailures.setFailure(13, 1);
        InjectedFailures.setFailure(9, 1);
        InjectedFailures.setFailure(10, 1);
        InjectedFailures.setFailure(5, 1);
        InjectedFailures.setFailure(8, 1);

        var op = privateClient.destroy(makeChannelDestroyCommand(channel.getChannelId()));

        skipAndRestoreOperation(op.getId(), 5);

        awaitOperationResponse(op.getId());
    }

    @Test
    public void testDestroyAllRestart() {
        final String executionId = UUID.randomUUID().toString();

        prepareChannelOfShape(executionId, "channel1", 1, 0, 0, 1);
        prepareChannelOfShape(executionId, "channel2", 1, 0, 0, 2);
        prepareChannelOfShape(executionId, "channel3", 0, 1, 1, 1);

        InjectedFailures.setFailure(14, 1);
        InjectedFailures.setFailure(9, 2);
        InjectedFailures.setFailure(10, 2);

        var op = privateClient.destroyAll(makeChannelDestroyAllCommand(executionId));

        skipAndRestoreOperation(op.getId(), 3);

        awaitOperationResponse(op.getId());
    }

    private LCM.Channel prepareChannelOfShape(String executionId, String channelName,
                                              int expectedPortalSenders, int expectedWorkerSenders,
                                              int expectedPortalReceivers, int expectedWorkerReceivers)
    {
        var channelCreateResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        String channelId = channelCreateResponse.getChannelId();

        List<LongRunning.Operation> operations = new ArrayList<>();
        if (expectedPortalReceivers != 0) {
            assertEquals(1, expectedPortalReceivers);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "inSlotP",
                LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.PORTAL));
            operations.add(op);
        }
        if (expectedWorkerReceivers != 0) {
            for (int i = 1; i <= expectedWorkerReceivers; ++i) {
                var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "inSlotW" + i,
                    LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER));
                operations.add(op);
            }
        }
        if (expectedPortalSenders != 0) {
            assertEquals(1, expectedPortalSenders);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "outSlotP",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.PORTAL));
            operations.add(op);
        }
        if (expectedWorkerSenders != 0) {
            assertEquals(1, expectedWorkerSenders);
            var op = publicClient.bind(makeBindCommand(channelId, "tid-" + channelName, "outSlotW",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER));
            operations.add(op);
        }

        operations.forEach(op -> awaitOperationResponse(op.getId()));

        var status = privateClient.status(makeChannelStatusCommand(channelId));
        assertChannelShape(
            expectedPortalSenders, expectedWorkerSenders,
            expectedPortalReceivers, expectedWorkerReceivers,
            status.getStatus().getChannel());

        return status.getStatus().getChannel();
    }

    private void skipAndRestoreOperation(String operationId, int skipTimes) {
        for (int i = 1; i <= skipTimes; ++i) {
            var op = awaitOperationDone(operationApiClient, operationId, Duration.ofMillis(300));
            assertFalse("Expected undone operation on attempt " + i, op.getDone());
            restoreActiveOperations();
        }
    }

}
