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
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ForkJoinPool;

public abstract class LzyOutputSlotBase extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(LzyOutputSlotBase.class);
    private SlotWriter slotWriter;

    protected LzyOutputSlotBase(Slot definition) {
        super(definition);
    }

    private class SlotWriter implements Closeable {
        private final URI slotUri;
        private final ManagedChannel servantSlotCh;
        private long offset = 0;
        private final StreamObserver<Servant.SendSlotDataMessage> responseObserver;
        private final StreamObserver<Servant.ReceivedDataStatus> statusReceiver = new StreamObserver<>() {
            @Override
            public void onNext(Servant.ReceivedDataStatus receivedDataStatus) {
                LOG.info("Got response for slot " + LzyOutputSlotBase.this + " sending " + JsonUtils.printRequest(receivedDataStatus));
                offset = receivedDataStatus.getOffset();
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Exception while sending chunks from slot " + LzyOutputSlotBase.this + ": " + throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Sending chunks from slot " + LzyOutputSlotBase.this + " was finished");
            }
        };

        SlotWriter(URI slotUri) {
            this.slotUri = slotUri;
            servantSlotCh = ManagedChannelBuilder.forAddress(
                slotUri.getHost(),
                slotUri.getPort()
            ).usePlaintext().build();
            final LzyServantGrpc.LzyServantStub connectedSlotController = LzyServantGrpc.newStub(servantSlotCh);
            responseObserver = connectedSlotController.writeToInputSlot(statusReceiver);
        }

        public void write() {
            try {
                responseObserver.onNext(Servant.SendSlotDataMessage.newBuilder()
                    .setRequest(Servant.SlotRequest.newBuilder()
                        .setSlot(slotUri.getPath())
                        .setOffset(offset)
                        .build())
                    .build());
                readFromPosition(offset).forEach(chunk -> responseObserver.onNext(createChunkMessage(chunk)));
                responseObserver.onNext(createEosMessage());
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                responseObserver.onError(iae);
            }
        }

        @Override
        public void close() throws IOException {
            servantSlotCh.shutdown();
        }
    }

    @Override
    public void connect(URI slotUri) {
        slotWriter = new SlotWriter(slotUri);
        ForkJoinPool.commonPool().execute(() -> {
            waitForState(Operations.SlotStatus.State.OPEN);
            slotWriter.write();
        });
    }

    @Override
    public void disconnect() {
        if (slotWriter != null) {
            try {
                slotWriter.close();
            } catch (IOException e) {
                LOG.error("Exception while closing LzyOutputSlotBase " + this + " exc: " + e);
            }
        }
        suspend();
    }

    private static Servant.SendSlotDataMessage createChunkMessage(ByteString chunk) {
        return Servant.SendSlotDataMessage.newBuilder().setMessage(
            Servant.Message.newBuilder().setChunk(chunk).build()).build();
    }

    private static Servant.SendSlotDataMessage createEosMessage() {
        return Servant.SendSlotDataMessage.newBuilder().setMessage(
            Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build()).build();
    }
}
