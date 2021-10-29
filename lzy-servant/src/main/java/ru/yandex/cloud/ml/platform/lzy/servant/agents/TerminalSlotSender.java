package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ReceivedDataStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.SendSlotDataMessage;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ForkJoinPool;


public class TerminalSlotSender {
    private static final Logger LOG = LogManager.getLogger(TerminalSlotSender.class);

    private class SlotWriter implements AutoCloseable {
        private final LzyOutputSlot lzySlot;
        private final URI slotUri;
        private final ManagedChannel servantSlotCh;
        private long offset = 0;
        private final StreamObserver<SendSlotDataMessage> responseObserver;

        SlotWriter(LzyOutputSlot lzySlot, URI slotUri) {
            this.lzySlot = lzySlot;
            this.slotUri = slotUri;
            servantSlotCh = ManagedChannelBuilder.forAddress(
                slotUri.getHost(),
                slotUri.getPort()
            ).usePlaintext().build();
            final LzyKharonGrpc.LzyKharonStub connectedSlotController = LzyKharonGrpc.newStub(servantSlotCh);
            final StreamObserver<ReceivedDataStatus> statusReceiver = new StreamObserver<>() {
                @Override
                public void onNext(ReceivedDataStatus receivedDataStatus) {
                    LOG.info("Got response for slot " + TerminalSlotSender.this + " sending " + JsonUtils.printRequest(receivedDataStatus));
                    offset = receivedDataStatus.getOffset();
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error("Exception while sending chunks from slot " + TerminalSlotSender.this + ": " + throwable);
                }

                @Override
                public void onCompleted() {
                    LOG.info("Sending chunks from slot " + TerminalSlotSender.this + " was finished");
                }
            };
            responseObserver = connectedSlotController.writeToInputSlot(statusReceiver);
        }

        public void run() {
            try {
                LOG.info("Starting sending bytes SlotWriter :: " + lzySlot);
                responseObserver.onNext(SendSlotDataMessage.newBuilder()
                    .setRequest(Servant.SlotRequest.newBuilder()
                        .setSlot(slotUri.getPath())
                        .setOffset(offset)
                        .setSlotUri(slotUri.toString())
                        .build())
                    .build());
                lzySlot.readFromPosition(offset).forEach(chunk -> responseObserver.onNext(createChunkMessage(chunk)));
                LOG.info("Completed sending bytes SlotWriter :: " + lzySlot);
                responseObserver.onNext(createEosMessage());
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                LOG.error("Got exception while sending bytes SlotWriter :: " + lzySlot + " exception:" + iae);
                responseObserver.onError(iae);
            }
        }

        @Override
        public void close() throws IOException {
            servantSlotCh.shutdown();
        }
    }

    public void connect(LzyOutputSlot lzySlot, URI slotUri) {
        LOG.info("TerminalOutputSlot::connect " + slotUri);
        try (SlotWriter writer = new SlotWriter(lzySlot, slotUri)) {
            ForkJoinPool.commonPool().execute(writer::run);
        } catch (IOException e) {
            LOG.error("Failed to send slot " + lzySlot + " cause: " + e);
        }
    }

    private static SendSlotDataMessage createChunkMessage(ByteString chunk) {
        return SendSlotDataMessage.newBuilder().setMessage(
            Servant.Message.newBuilder().setChunk(chunk).build()).build();
    }

    private static SendSlotDataMessage createEosMessage() {
        return SendSlotDataMessage.newBuilder().setMessage(
            Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build()).build();
    }

}
