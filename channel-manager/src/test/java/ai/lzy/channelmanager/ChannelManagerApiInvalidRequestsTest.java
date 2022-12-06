package ai.lzy.channelmanager;

import ai.lzy.model.DataScheme;
import ai.lzy.v1.channel.v2.LCM.ChannelSpec;
import ai.lzy.v1.channel.v2.LCMPS.ChannelCreateRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyAllRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelDestroyRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusAllRequest;
import ai.lzy.v1.channel.v2.LCMPS.ChannelStatusRequest;
import ai.lzy.v1.channel.v2.LCMS.BindRequest;
import ai.lzy.v1.channel.v2.LCMS.UnbindRequest;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ChannelManagerApiInvalidRequestsTest extends ChannelManagerApiBaseTest {

    protected LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub unauthorizedPrivateClient;
    protected LzyChannelManagerGrpc.LzyChannelManagerBlockingStub unauthorizedPublicClient;

    @Before
    public void before() throws IOException, InterruptedException {
        super.before();

        unauthorizedPrivateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
            "NoAuthPrivateClientTest", null);

        unauthorizedPublicClient = newBlockingClient(LzyChannelManagerGrpc.newBlockingStub(channel),
            "NoAuthPublicClientTest", null);
    }

    @After
    public void after() throws InterruptedException {
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        try {
            unauthorizedPrivateClient.create(ChannelCreateRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.destroy(ChannelDestroyRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.destroyAll(ChannelDestroyAllRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.status(ChannelStatusRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPrivateClient.statusAll(ChannelStatusAllRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPublicClient.bind(BindRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        try {
            unauthorizedPublicClient.unbind(UnbindRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testInvalidArgument() {
        testInvalidCreate(request -> request.toBuilder().clearExecutionId().build());

        testInvalidCreate(request -> request.toBuilder().clearChannelSpec().build());

        testInvalidCreate(request -> request.toBuilder()
            .setChannelSpec(request.getChannelSpec().toBuilder().clearChannelName().build())
            .build());

        testInvalidCreate(request -> request.toBuilder()
            .setChannelSpec(request.getChannelSpec().toBuilder().clearScheme().build())
            .build());

        testInvalidCreate(request -> request.toBuilder()
            .setChannelSpec(request.getChannelSpec().toBuilder()
                .setScheme(request.getChannelSpec().getScheme().toBuilder().clearSchemeFormat().build())
                .build())
            .build());

        testInvalidCreate(request -> request.toBuilder()
            .setChannelSpec(request.getChannelSpec().toBuilder()
                .setScheme(request.getChannelSpec().getScheme().toBuilder().clearDataFormat().build())
                .build())
            .build());


        testInvalidDestroy(request -> request.toBuilder().clearChannelId().build());


        testInvalidDestroyAll(request -> request.toBuilder().clearExecutionId().build());


        testInvalidStatus(request -> request.toBuilder().clearChannelId().build());


        testInvalidStatusAll(request -> request.toBuilder().clearExecutionId().build());


        testInvalidBind(request -> request.toBuilder().clearSlotOwner().build());

        testInvalidBind(request -> request.toBuilder().clearSlotInstance().build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder().clearChannelId().build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder().clearTaskId().build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder().clearSlotUri().build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder()
                .setSlot(request.getSlotInstance().getSlot().toBuilder().clearName().build())
                .build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder()
                .setSlot(request.getSlotInstance().getSlot().toBuilder().clearDirection().build())
                .build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder()
                .setSlot(request.getSlotInstance().getSlot().toBuilder().clearContentType().build())
                .build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder()
                .setSlot(request.getSlotInstance().getSlot().toBuilder()
                    .setContentType(request.getSlotInstance().getSlot().getContentType().toBuilder()
                        .clearSchemeFormat()
                        .build())
                    .build())
                .build())
            .build());

        testInvalidBind(request -> request.toBuilder()
            .setSlotInstance(request.getSlotInstance().toBuilder()
                .setSlot(request.getSlotInstance().getSlot().toBuilder()
                    .setContentType(request.getSlotInstance().getSlot().getContentType().toBuilder()
                        .clearDataFormat()
                        .build())
                    .build())
                .build())
            .build());


        testInvalidUnbind(request -> request.toBuilder().clearSlotUri().build());
    }

    private void testInvalidCreate(Function<ChannelCreateRequest, ChannelCreateRequest> requestCorrupter) {
        try {
            var request = ChannelCreateRequest.newBuilder()
                .setExecutionId(UUID.randomUUID().toString())
                .setChannelSpec(ChannelSpec.newBuilder()
                    .setChannelName("chanel_name")
                    .setScheme(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                    .build())
                .build();
            privateClient.create(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidDestroy(Function<ChannelDestroyRequest, ChannelDestroyRequest> requestCorrupter) {
        try {
            var request = ChannelDestroyRequest.newBuilder().setChannelId("id").build();
            privateClient.destroy(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidDestroyAll(Function<ChannelDestroyAllRequest, ChannelDestroyAllRequest> requestCorrupter) {
        try {
            var request = ChannelDestroyAllRequest.newBuilder().setExecutionId("id").build();
            privateClient.destroyAll(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidStatus(Function<ChannelStatusRequest, ChannelStatusRequest> requestCorrupter) {
        try {
            var request = ChannelStatusRequest.newBuilder().setChannelId("id").build();
            privateClient.status(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidStatusAll(Function<ChannelStatusAllRequest, ChannelStatusAllRequest> requestCorrupter) {
        try {
            var request = ChannelStatusAllRequest.newBuilder().setExecutionId("id").build();
            privateClient.statusAll(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidBind(Function<BindRequest, BindRequest> requestCorrupter) {
        try {
            var request = BindRequest.newBuilder()
                .setSlotInstance(LMS.SlotInstance.newBuilder()
                    .setChannelId("channel_id")
                    .setTaskId("tid")
                    .setSlotUri("slot_uri")
                    .setSlot(LMS.Slot.newBuilder()
                        .setName("slot_name")
                        .setDirection(LMS.Slot.Direction.INPUT)
                        .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(DataScheme.PLAIN))
                        .build())
                    .build())
                .setSlotOwner(BindRequest.SlotOwner.WORKER)
                .build();
            publicClient.bind(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    private void testInvalidUnbind(Function<UnbindRequest, UnbindRequest> requestCorrupter) {
        try {
            var request = UnbindRequest.newBuilder().setSlotUri("uri").build();
            publicClient.unbind(requestCorrupter.apply(request));
            fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }



}

