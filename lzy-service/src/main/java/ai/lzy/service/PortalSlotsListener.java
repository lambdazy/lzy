package ai.lzy.service;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsGrpc;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PortalSlotsListener extends Thread {
    private static final Logger LOG = LogManager.getLogger(PortalSlotsListener.class);

    private final HostAndPort portalAddress;
    private final String portalId;
    private final String slotName;
    private final Consumer<ByteString> consumer;


    public PortalSlotsListener(
        HostAndPort portalAddress,
        String portalId,
        String slotName,
        Consumer<ByteString> consumer
    )
    {
        this.portalAddress = portalAddress;
        this.portalId = portalId;
        this.slotName = slotName;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        var channel = ChannelBuilder.forAddress(portalAddress)
            .usePlaintext()
            .enableRetry(LzyFsGrpc.SERVICE_NAME)
            .build();
        try {

            var stub = LzyFsGrpc.newBlockingStub(channel);

            var stream = stub.openOutputSlot(
                LzyFsApi.SlotRequest.newBuilder()
                    .setSlotInstance(LMS.SlotInstance.newBuilder()
                        .setTaskId(portalId)
                        .setSlotUri("fs://some.portal.slot/")
                        .setSlot(
                            LMS.Slot.newBuilder()
                                .setName(slotName)
                                .build()
                        )
                        .build())
                    .setOffset(0)
                    .build()
            );

            while (stream.hasNext()) {
                var msg = stream.next();
                if (msg.hasControl() && msg.getControl() == LzyFsApi.Message.Controls.EOS) {
                    return;
                }
                consumer.accept(msg.getChunk());
            }

        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Cannot terminate channel", e);
            }
        }

    }
}
