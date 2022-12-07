package ai.lzy.channelmanager;

import ai.lzy.v1.channel.v2.LCM.Channel;
import ai.lzy.v1.channel.v2.LCM.ChannelReceivers;
import ai.lzy.v1.channel.v2.LCM.ChannelSenders;
import ai.lzy.v1.channel.v2.LCMPS.ChannelCreateResponse;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatus;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusResponse;
import ai.lzy.v1.channel.v2.LCMS.BindRequest;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ChannelManagerApiTest extends ChannelManagerBaseApiTest {

    @Before
    public void before() throws IOException, InterruptedException {
        super.before();
    }

    @After
    public void after() throws InterruptedException {
        super.after();
    }

    @Test
    public void testCreateDestroy() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        final ChannelCreateResponse channelCreateResponse = privateClient.create(
            makeChannelCreateCommand(executionId, channelName));
        final String channelId = channelCreateResponse.getChannelId();
        assertTrue(channelId.length() > 1);

        final ChannelStatusResponse channelStatusResponse = privateClient.status(
            ChannelStatusRequest.newBuilder().setChannelId(channelId).build());
        final var expectedChannelStatus = ChannelStatus.newBuilder()
            .setChannel(Channel.newBuilder()
                .setChannelId(channelId)
                .setSpec(makeChannelSpec(channelName))
                .setExecutionId(executionId)
                .setSenders(ChannelSenders.getDefaultInstance())
                .setReceivers(ChannelReceivers.getDefaultInstance())
                .build())
            .build();
        assertEquals(expectedChannelStatus, channelStatusResponse.getStatus());

        LongRunning.Operation destroyOp = privateClient.destroy(makeChannelDestroyCommand(channelId));
        awaitOperationResponse(destroyOp.getId());

        try {
            privateClient.status(makeChannelStatusCommand(channelId));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testDestroyNonexistentChannel() {
        final String channelId = UUID.randomUUID().toString();
        LongRunning.Operation destroyOp = privateClient.destroy(makeChannelDestroyCommand(channelId));
        destroyOp = awaitOperationDone(operationApiClient, destroyOp.getId(),
            Duration.of(10, ChronoUnit.SECONDS));
        assertTrue(destroyOp.hasResponse());
    }

    @Test
    public void testDestroyAll() {
        final String executionId = UUID.randomUUID().toString();
        final ChannelCreateResponse ch1Response = privateClient.create(
            makeChannelCreateCommand(executionId, "ch1"));
        final ChannelCreateResponse ch2Response = privateClient.create(
            makeChannelCreateCommand(executionId, "ch2"));
        final ChannelCreateResponse ch0Response = privateClient.create(
            makeChannelCreateCommand(UUID.randomUUID().toString(), "ch0"));

        LongRunning.Operation destroyOp = privateClient.destroyAll(makeChannelDestroyAllCommand(executionId));
        awaitOperationResponse(destroyOp.getId());

        try {
            privateClient.status(makeChannelStatusCommand(ch1Response.getChannelId()));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        try {
            privateClient.status(makeChannelStatusCommand(ch2Response.getChannelId()));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
        final var status = privateClient.status(makeChannelStatusCommand(ch0Response.getChannelId()));
        assertEquals(ch0Response.getChannelId(), status.getStatus().getChannel().getChannelId());
    }

    @Test
    public void testOneSenderOneReceiver() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        final ChannelCreateResponse chResponse = privateClient.create(
            makeChannelCreateCommand(executionId, channelName));

        BindRequest bindRequest = makeBindCommand(chResponse.getChannelId(), "inSlot",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOp = publicClient.bind(bindRequest);
        String inputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        bindRequest = makeBindCommand(chResponse.getChannelId(), "outSlot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation binOpSender = publicClient.bind(bindRequest);
        String outputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        awaitOperationResponse(bindOp.getId());
        awaitOperationResponse(binOpSender.getId());

        var status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 1, 0, 1, status.getStatus().getChannel());

        LongRunning.Operation unbindOp = publicClient.unbind(makeUnbindCommand(inputSlotUri));
        awaitOperationResponse(unbindOp.getId());

        status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 1, 0, 0, status.getStatus().getChannel());

        LongRunning.Operation unbindOpSender = publicClient.unbind(makeUnbindCommand(outputSlotUri));
        awaitOperationResponse(unbindOpSender.getId());

        status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 0, 0, 0, status.getStatus().getChannel());
    }

    @Test
    public void testOneSenderManyReceivers() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        final ChannelCreateResponse chResponse = privateClient.create(
            makeChannelCreateCommand(executionId, channelName));

        BindRequest bindRequest = makeBindCommand(chResponse.getChannelId(), "inSlot1",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOp1 = publicClient.bind(bindRequest);
        String inputSlot1Uri = bindRequest.getSlotInstance().getSlotUri();

        bindRequest = makeBindCommand(chResponse.getChannelId(), "inSlot2",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOp2 = publicClient.bind(bindRequest);
        String inputSlot2Uri = bindRequest.getSlotInstance().getSlotUri();

        bindRequest = makeBindCommand(chResponse.getChannelId(), "inSlotP",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.PORTAL);
        LongRunning.Operation bindOpP = publicClient.bind(bindRequest);
        String inputSlotPUri = bindRequest.getSlotInstance().getSlotUri();

        bindRequest = makeBindCommand(chResponse.getChannelId(), "outSlot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOpSender = publicClient.bind(bindRequest);
        String outputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        bindRequest = makeBindCommand(chResponse.getChannelId(), "inSlot3",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOp3 = publicClient.bind(bindRequest);
        String inputSlot3Uri = bindRequest.getSlotInstance().getSlotUri();

        awaitOperationResponse(bindOp1.getId());
        awaitOperationResponse(bindOp2.getId());
        awaitOperationResponse(bindOp3.getId());
        awaitOperationResponse(bindOpP.getId());
        awaitOperationResponse(bindOpSender.getId());

        var status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 1, 1, 3, status.getStatus().getChannel());

        LongRunning.Operation unbindOp3 = publicClient.unbind(makeUnbindCommand(inputSlot3Uri));
        LongRunning.Operation unbindOpP = publicClient.unbind(makeUnbindCommand(inputSlotPUri));
        awaitOperationResponse(unbindOp3.getId());
        awaitOperationResponse(unbindOpP.getId());

        status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 1, 0, 2, status.getStatus().getChannel());

        LongRunning.Operation unbindOpSender = publicClient.unbind(makeUnbindCommand(outputSlotUri));
        awaitOperationResponse(unbindOpSender.getId());

        status = privateClient.status(makeChannelStatusCommand(chResponse.getChannelId()));
        assertChannelShape(0, 0, 0, 0, status.getStatus().getChannel());
    }

    @Test
    public void testForbiddenSecondWorker() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        final ChannelCreateResponse chResponse = privateClient.create(
            makeChannelCreateCommand(executionId, channelName));

        final var bindOutputRequest = makeBindCommand(chResponse.getChannelId(), "worker_slot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        LongRunning.Operation bindOutputOp = publicClient.bind(bindOutputRequest);
        awaitOperationResponse(bindOutputOp.getId());

        try {
            publicClient.bind(makeBindCommand(chResponse.getChannelId(), "worker_slot_2_output",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testForbiddenSecondPortal() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        final ChannelCreateResponse chResponse = privateClient.create(
            makeChannelCreateCommand(executionId, channelName));

        final var bindOutputRequest = makeBindCommand(chResponse.getChannelId(), "portal_slot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.PORTAL);
        LongRunning.Operation bindOutputOp = publicClient.bind(bindOutputRequest);
        awaitOperationResponse(bindOutputOp.getId());

        try {
            publicClient.bind(makeBindCommand(chResponse.getChannelId(), "portal_slot_2_input",
                LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.PORTAL));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
        }

        try {
            publicClient.bind(makeBindCommand(chResponse.getChannelId(), "portal_slot_2_output",
                LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.PORTAL));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.FAILED_PRECONDITION.getCode(), e.getStatus().getCode());
        }
    }

}
