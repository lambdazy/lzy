package ai.lzy.channelmanager;

import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.v1.channel.LCM.Channel;
import ai.lzy.v1.channel.LCM.ChannelReceivers;
import ai.lzy.v1.channel.LCM.ChannelSenders;
import ai.lzy.v1.channel.LCMPS.ChannelCreateResponse;
import ai.lzy.v1.channel.LCMPS.ChannelStatus;
import ai.lzy.v1.channel.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.LCMPS.ChannelStatusResponse;
import ai.lzy.v1.channel.LCMS.BindRequest;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;

import static ai.lzy.longrunning.OperationGrpcServiceUtils.awaitOperationDone;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ChannelManagerApiTest extends ChannelManagerBaseApiTest {

    @Before
    public void before() throws Exception {
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
    public void testCreateTwice() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        final var createCmd = makeChannelCreateCommand(executionId, channelName);
        privateClient.create(createCmd);
        try {
            privateClient.create(createCmd);
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.ALREADY_EXISTS.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testBindTwice() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        final var bindCmd = makeBindCommand(channelId, "inSlotW",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);

        publicClient.bind(bindCmd);
        try {
            publicClient.bind(bindCmd);
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.ALREADY_EXISTS.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void idempotentConcurrentBind() throws Exception {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        final var bindCmd = makeBindCommand(channelId, "inSlotW",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);

        var idempotentClient = withIdempotencyKey(publicClient, bindCmd.getSlotInstance().getSlotUri());

        idempotentConcurrentOperationTest(10, () -> idempotentClient.bind(bindCmd));
    }

    @Test
    public void testUnbindTwice() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        final var bindCmd = makeBindCommand(channelId, "inSlotW",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        var op = publicClient.bind(bindCmd);
        awaitOperationResponse(op.getId());

        final var unbindCmd = makeUnbindCommand(bindCmd.getSlotInstance().getSlotUri());
        publicClient.unbind(unbindCmd);
        try {
            publicClient.unbind(unbindCmd);
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void idempotentConcurrentUnbind() throws Exception {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        final var bindCmd = makeBindCommand(channelId, "inSlotW",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        var op = publicClient.bind(bindCmd);
        awaitOperationResponse(op.getId());

        final var unbindCmd = makeUnbindCommand(bindCmd.getSlotInstance().getSlotUri());

        var idempotentClient = withIdempotencyKey(publicClient, bindCmd.getSlotInstance().getSlotUri());

        idempotentConcurrentOperationTest(10, () -> idempotentClient.unbind(unbindCmd));
    }

    @Test
    public void testDestroyTwice() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        var op1 = privateClient.destroy(makeChannelDestroyCommand(channelId));
        var op2 = privateClient.destroy(makeChannelDestroyCommand(channelId));

        awaitOperationResponse(op1.getId());
        awaitOperationResponse(op2.getId());
    }

    @Test
    public void idempotentConcurrentDestroy() throws Exception {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch1";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        final var destroyCmd = makeChannelDestroyCommand(channelId);

        var idempotentClient = withIdempotencyKey(privateClient, channelId);

        idempotentConcurrentOperationTest(10, () -> idempotentClient.destroy(destroyCmd));
    }

    @Test
    public void testDestroyAll() {
        final String executionId = UUID.randomUUID().toString();
        var channel1 = prepareChannelOfShape(executionId, "ch1", 0, 1, 0, 1);
        var channel2 = prepareChannelOfShape(executionId, "ch2", 0, 1, 1, 0);
        var channel3 = prepareChannelOfShape(executionId, "ch3", 1, 0, 0, 1);

        var channel = prepareChannelOfShape(UUID.randomUUID().toString(), "ch", 0, 1, 0, 1);

        LongRunning.Operation destroyOp = privateClient.destroyAll(makeChannelDestroyAllCommand(executionId));
        awaitOperationResponse(destroyOp.getId());

        try {
            privateClient.status(makeChannelStatusCommand(channel1.getChannelId()));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }

        final var status = privateClient.status(makeChannelStatusCommand(channel.getChannelId()));
        assertEquals(channel.getChannelId(), status.getStatus().getChannel().getChannelId());

        var statusAll = privateClient.statusAll(makeChannelStatusAllCommand(executionId));
        assertEquals(0, statusAll.getStatusesList().size());
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

    @Test
    public void unbindThisWhileBind() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        BindRequest bindRequest = makeBindCommand(channelId, "inSlot",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        var bindOp = publicClient.bind(bindRequest);
        String inputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        awaitOperationResponse(bindOp.getId());

        int marker = 1;
        InjectedFailures.setFailure(marker, 1);

        BindRequest bindRequestSender = makeBindCommand(channelId, "outSlot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        var bindOpSender = publicClient.bind(bindRequestSender);
        String outputSlotUri = bindRequestSender.getSlotInstance().getSlotUri();

        boolean failed = InjectedFailures.awaitFailure(marker);
        assertTrue(failed);

        var unbindOp = publicClient.unbind(makeUnbindCommand(outputSlotUri));
        awaitOperationResponse(unbindOp.getId());

        restoreActiveOperations();

        bindOpSender = awaitOperationDone(operationApiClient, bindOpSender.getId(), Duration.ofSeconds(10));
        assertEquals(bindOpSender.getError().toString(),
            Status.CANCELLED.getCode().value(), bindOpSender.getError().getCode());
    }

    @Test
    public void unbindConnectedWhileBind() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        BindRequest bindRequest = makeBindCommand(channelId, "inSlot",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        var bindOp = publicClient.bind(bindRequest);
        String inputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        awaitOperationResponse(bindOp.getId());

        int marker = 1;
        InjectedFailures.setFailure(marker, 1);

        BindRequest bindRequestSender = makeBindCommand(channelId, "outSlot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        var bindOpSender = publicClient.bind(bindRequestSender);
        String outputSlotUri = bindRequestSender.getSlotInstance().getSlotUri();

        boolean failed = InjectedFailures.awaitFailure(marker);
        assertTrue(failed);

        var unbindOp = publicClient.unbind(makeUnbindCommand(inputSlotUri));
        awaitOperationResponse(unbindOp.getId());

        restoreActiveOperations();

        awaitOperationResponse(bindOpSender.getId());
    }

    @Test
    public void destroyChannelWhileBind() {
        final String executionId = UUID.randomUUID().toString();
        final String channelName = "ch";
        var createResponse = privateClient.create(makeChannelCreateCommand(executionId, channelName));
        final String channelId = createResponse.getChannelId();

        BindRequest bindRequest = makeBindCommand(channelId, "inSlot",
            LMS.Slot.Direction.INPUT, BindRequest.SlotOwner.WORKER);
        var bindOp = publicClient.bind(bindRequest);
        String inputSlotUri = bindRequest.getSlotInstance().getSlotUri();

        awaitOperationResponse(bindOp.getId());

        int marker = 1;
        InjectedFailures.setFailure(marker, 1);

        BindRequest bindRequestSender = makeBindCommand(channelId, "outSlot",
            LMS.Slot.Direction.OUTPUT, BindRequest.SlotOwner.WORKER);
        var bindOpSender = publicClient.bind(bindRequestSender);
        String outputSlotUri = bindRequestSender.getSlotInstance().getSlotUri();

        boolean failed = InjectedFailures.awaitFailure(marker);
        assertTrue(failed);

        var destroyOp = privateClient.destroy(makeChannelDestroyCommand(channelId));
        awaitOperationResponse(destroyOp.getId());

        restoreActiveOperations();

        bindOpSender = awaitOperationDone(operationApiClient, bindOpSender.getId(), Duration.ofSeconds(10));
        assertEquals(bindOpSender.getError().toString(),
            Status.CANCELLED.getCode().value(), bindOpSender.getError().getCode());
    }

}
