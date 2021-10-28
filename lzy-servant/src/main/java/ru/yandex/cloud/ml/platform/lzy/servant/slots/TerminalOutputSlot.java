package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ReceivedDataStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.SendSlotDataMessage;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ForkJoinPool;


public class TerminalOutputSlot extends LzySlotBase {
    private static final Logger LOG = LogManager.getLogger(TerminalOutputSlot.class);
    private final LzyOutputSlot lzySlot;
    private SlotWriter slotWriter;

    public TerminalOutputSlot(Slot definition, LzyOutputSlot lzySlot) {
        super(definition);
        this.lzySlot = lzySlot;
    }

    private class SlotWriter implements Closeable {
        private final URI slotUri;
        private final ManagedChannel servantSlotCh;
        private long offset = 0;
        private final StreamObserver<SendSlotDataMessage> responseObserver;

        SlotWriter(URI slotUri) {
            this.slotUri = slotUri;
            servantSlotCh = ManagedChannelBuilder.forAddress(
                slotUri.getHost(),
                slotUri.getPort()
            ).usePlaintext().build();
            final LzyKharonGrpc.LzyKharonStub connectedSlotController = LzyKharonGrpc.newStub(servantSlotCh);
            final StreamObserver<ReceivedDataStatus> statusReceiver = new StreamObserver<>() {
                @Override
                public void onNext(ReceivedDataStatus receivedDataStatus) {
                    LOG.info("Got response for slot " + TerminalOutputSlot.this + " sending " + JsonUtils.printRequest(receivedDataStatus));
                    offset = receivedDataStatus.getOffset();
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error("Exception while sending chunks from slot " + TerminalOutputSlot.this + ": " + throwable);
                }

                @Override
                public void onCompleted() {
                    LOG.info("Sending chunks from slot " + TerminalOutputSlot.this + " was finished");
                }
            };
            responseObserver = connectedSlotController.writeToInputSlot(statusReceiver);
        }

        public void write() {
            try {
                responseObserver.onNext(SendSlotDataMessage.newBuilder()
                    .setRequest(Servant.SlotRequest.newBuilder()
                        .setSlot(slotUri.getPath())
                        .setOffset(offset)
                        .build())
                    .build());
                lzySlot.readFromPosition(offset).forEach(chunk -> responseObserver.onNext(createChunkMessage(chunk)));
                LOG.info("Completed sending bytes LzyOutputSlotBase :: " + this);
                responseObserver.onNext(createEosMessage());
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                LOG.error("Got exception while sending bytes LzyOutputSlotBase :: " + this + " exception:" + iae);
                responseObserver.onError(iae);
            }
        }

        @Override
        public void close() throws IOException {
            servantSlotCh.shutdown();
        }
    }

    public void connect(URI slotUri) {
        slotWriter = new SlotWriter(slotUri);
        ForkJoinPool.commonPool().execute(() -> {
            waitForState(Operations.SlotStatus.State.OPEN);
            slotWriter.write();
        });
    }

    public void disconnect() {
        if (slotWriter != null) {
            try {
                slotWriter.close();
            } catch (IOException e) {
                LOG.error("Exception while closing TerminalOutputSlot " + this + " exc: " + e);
            }
        }
        suspend();
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
