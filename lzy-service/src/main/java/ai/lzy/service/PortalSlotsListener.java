package ai.lzy.service;

import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.lzy.portal.Portal.PORTAL_ERR_SLOT_NAME;
import static ai.lzy.portal.Portal.PORTAL_OUT_SLOT_NAME;

public class PortalSlotsListener {
    private static final Logger LOG = LogManager.getLogger(PortalSlotsListener.class);

    private final String portalId;
    private final StreamObserver<LWFS.ReadStdSlotsResponse> consumer;
    private final ManagedChannel channel;
    private final LzyFsGrpc.LzyFsBlockingStub stub;

    private final AtomicInteger openedCalls = new AtomicInteger(2);
    private final ClientCall<LzyFsApi.SlotRequest, LzyFsApi.Message> outCall;
    private final ClientCall<LzyFsApi.SlotRequest, LzyFsApi.Message> errCall;


    public PortalSlotsListener(
        HostAndPort portalAddress,
        String portalId,
        StreamObserver<LWFS.ReadStdSlotsResponse> consumer
    )
    {
        this.portalId = portalId;
        this.consumer = consumer;

        channel = GrpcUtils.newGrpcChannel(portalAddress, LzyFsGrpc.SERVICE_NAME);
        stub = GrpcUtils.newBlockingClient(LzyFsGrpc.newBlockingStub(channel), "portal-fs-client", null);

        outCall = createCall(PORTAL_OUT_SLOT_NAME, msg -> consumer.onNext(
            LWFS.ReadStdSlotsResponse.newBuilder()
                .setStdout(
                    LWFS.ReadStdSlotsResponse.Data.newBuilder()
                        .addData(msg.toStringUtf8())
                        .build()
                ).build()));

        errCall = createCall(PORTAL_ERR_SLOT_NAME, msg -> consumer.onNext(
            LWFS.ReadStdSlotsResponse.newBuilder()
                .setStderr(
                    LWFS.ReadStdSlotsResponse.Data.newBuilder()
                        .addData(msg.toStringUtf8())
                        .build()
                ).build()));
    }

    public ClientCall<LzyFsApi.SlotRequest, LzyFsApi.Message> createCall(String slotName, Consumer<ByteString> consumer) {
        var call = stub.getChannel().newCall(LzyFsGrpc.getOpenOutputSlotMethod(), stub.getCallOptions());

        var listener = new Listener<LzyFsApi.Message>() {
            @Override
            public void onMessage(LzyFsApi.Message message) {
                if (message.hasControl() && message.getControl() == LzyFsApi.Message.Controls.EOS) {
                    LOG.info("Stream of portal <{}> slot <{}> is completed", portalId, slotName);
                    return;
                }
                consumer.accept(message.getChunk());
                call.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    LOG.error("Error while listening for portal slots: ", status.asException());
                }
                callClosed();
            }
        };

        call.start(listener, new Metadata());

        var msg = LzyFsApi.SlotRequest.newBuilder()
            .setSlotInstance(LMS.SlotInstance.newBuilder()
                .setTaskId(portalId)
                .setSlotUri("fs://some.portal.slot/")  // To mock real call to fs api.
                .setSlot(
                    LMS.Slot.newBuilder()
                        .setName(slotName)
                        .build()
                )
                .build())
            .setOffset(0)
            .build();

        call.sendMessage(msg);
        call.halfClose();
        call.request(1);
        return call;
    }

    private void callClosed() {
        if (openedCalls.decrementAndGet() == 0) {
            consumer.onCompleted();
            channel.shutdown();
            try {
                channel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Cannot terminate channel", e);
            }
        }
    }

    public void cancel(String issue) {
        LOG.info("Cancelling listener with issue: {}", issue);
        consumer.onError(Status.CANCELLED.asException());
        outCall.cancel(issue, null);
        errCall.cancel(issue, null);
    }
}
