package ai.lzy.service;

import ai.lzy.v1.common.LMS;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.lzy.portal.services.PortalService.PORTAL_ERR_SLOT_NAME;
import static ai.lzy.portal.services.PortalService.PORTAL_OUT_SLOT_NAME;
import static ai.lzy.util.grpc.GrpcUtils.NO_AUTH_TOKEN;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class PortalSlotsListener {
    private static final Logger LOG = LogManager.getLogger(PortalSlotsListener.class);

    private final String portalId;
    private final StreamObserver<LWFS.ReadStdSlotsResponse> consumer;
    private final ManagedChannel slotsChannel;
    private final LzySlotsApiGrpc.LzySlotsApiBlockingStub slotsApi;

    private final AtomicInteger openedCalls = new AtomicInteger(2);
    private final ClientCall<LSA.SlotDataRequest, LSA.SlotDataChunk> outCall;
    private final ClientCall<LSA.SlotDataRequest, LSA.SlotDataChunk> errCall;


    public PortalSlotsListener(HostAndPort portalAddress, String portalId,
                               ServerCallStreamObserver<LWFS.ReadStdSlotsResponse> consumer)
    {
        this.portalId = portalId;
        this.consumer = consumer;

        slotsChannel = newGrpcChannel(portalAddress, LzySlotsApiGrpc.SERVICE_NAME);
        slotsApi = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(slotsChannel), "PortalStdSlots", NO_AUTH_TOKEN);

        consumer.setOnCancelHandler(() -> {
            LOG.error("Lost connection to client, cancelling slot listener to portal {}", portalId);
            cancel("Cancelled by client");
        });

        outCall = createCall(PORTAL_OUT_SLOT_NAME, msg -> {
            synchronized (consumer) {  // Synchronized to prevent onNext from multiple calls
                consumer.onNext(
                    LWFS.ReadStdSlotsResponse.newBuilder()
                        .setStdout(
                            LWFS.ReadStdSlotsResponse.Data.newBuilder()
                                .addData(
                                    LWFS.ReadStdSlotsResponse.TaskLines.newBuilder()
                                        .setTaskId("")
                                        .setLines(msg.toStringUtf8())
                                        .build())
                                .build())
                        .build());
            }
        });

        errCall = createCall(PORTAL_ERR_SLOT_NAME, msg -> {
            synchronized (consumer) {  // Synchronized to prevent onNext from multiple calls
                consumer.onNext(
                    LWFS.ReadStdSlotsResponse.newBuilder()
                        .setStderr(
                            LWFS.ReadStdSlotsResponse.Data.newBuilder()
                                .addData(
                                    LWFS.ReadStdSlotsResponse.TaskLines.newBuilder()
                                        .setTaskId("")
                                        .setLines(msg.toStringUtf8())
                                        .build())
                                .build())
                        .build());
            }
        });
    }

    public ClientCall<LSA.SlotDataRequest, LSA.SlotDataChunk> createCall(String slotName, Consumer<ByteString> cons) {
        var call = slotsApi.getChannel().newCall(LzySlotsApiGrpc.getOpenOutputSlotMethod(), slotsApi.getCallOptions());

        var listener = new Listener<LSA.SlotDataChunk>() {
            @Override
            public void onMessage(LSA.SlotDataChunk message) {
                if (message.hasControl()) {
                    switch (message.getControl()) {
                        case EOS -> {
                            LOG.info("Stream of portal <{}> slot <{}> is completed", portalId, slotName);
                            return;
                        }
                        case UNRECOGNIZED -> {
                            LOG.error("Unexpected control value, portal <{}>, slot <{}>", portalId, slotName);
                            return;
                        }
                    }
                }

                LOG.debug("Got data, portal <{}>, slot <{}>: {} bytes", portalId, slotName, message.getChunk().size());

                try {
                    cons.accept(message.getChunk());
                } catch (Exception e) {
                    LOG.error("Error while sending chunk {} to client. Chunk will be lost. Cancelling call...",
                        message.getChunk().toStringUtf8(), e);
                    cancel("Error from client");
                    return;
                }

                call.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    LOG.error("Error while listening for portal <{}> slots: ", portalId, status.asException());
                }
                callClosed();
            }
        };

        call.start(listener, new Metadata());

        var msg = LSA.SlotDataRequest.newBuilder()
            .setSlotInstance(LMS.SlotInstance.newBuilder()
                .setTaskId(portalId)
                .setSlotUri("fs://portal-%s/slot-%s".formatted(portalId, slotName)) // just debug string
                .setSlot(
                    LMS.Slot.newBuilder()
                        .setName(slotName)
                        .build())
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
            complete();
            slotsChannel.shutdown();
            try {
                slotsChannel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Cannot terminate std slots channel", e);
            }
        }
    }

    public void cancel(String issue) {
        LOG.info("Cancelling listener with issue: {}", issue);
        consumer.onError(Status.CANCELLED.asException());
        outCall.cancel(issue, null);
        errCall.cancel(issue, null);
    }

    private void complete() {
        LOG.info("Completing listener");
        consumer.onCompleted();
        outCall.cancel("Completed", null);
        errCall.cancel("Completed", null);
    }
}
