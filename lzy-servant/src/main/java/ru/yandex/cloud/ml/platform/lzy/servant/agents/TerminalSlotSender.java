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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;


public class TerminalSlotSender {
    private static final Logger LOG = LogManager.getLogger(TerminalSlotSender.class);

    private class SlotWriter implements AutoCloseable {
        private final LzyOutputSlot lzySlot;
        private final URI slotUri;
        private final ManagedChannel servantSlotCh;
        private long offset = 0;
        private final StreamObserver<SendSlotDataMessage> responseObserver;
        private final CompletableFuture<Boolean> writingCompleted = new CompletableFuture<>();

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
                    LOG.info("Got response for slot " + lzySlot + " sending " + JsonUtils.printRequest(receivedDataStatus));
                    offset = receivedDataStatus.getOffset();
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error("Exception while sending chunks from slot " + lzySlot + ": " + throwable);
                }

                @Override
                public void onCompleted() {
                    LOG.info("Sending chunks from slot " + lzySlot + " was finished");
                    writingCompleted.complete(true);
                }
            };
            responseObserver = connectedSlotController.writeToInputSlot(statusReceiver);
        }

        public void run() {
            try {
                LOG.info("Starting sending bytes slot:: " + lzySlot);
                responseObserver.onNext(SendSlotDataMessage.newBuilder()
                    .setRequest(Servant.SlotRequest.newBuilder()
                        .setSlot(slotUri.getPath())
                        .setOffset(offset)
                        .setSlotUri(slotUri.toString())
                        .build())
                    .build());
                lzySlot.readFromPosition(offset).forEach(chunk -> responseObserver.onNext(createChunkMessage(chunk)));
                LOG.info("Completed sending bytes slot:: " + lzySlot);
                responseObserver.onNext(createEosMessage());
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                LOG.error("Got exception while sending bytes slot:: " + lzySlot + " exception:" + iae);
                responseObserver.onError(iae);
            }
        }

        @Override
        public void close() throws IOException, ExecutionException, InterruptedException {
            writingCompleted.get();
            servantSlotCh.shutdown();
        }
    }

    public void connect(LzyOutputSlot lzySlot, URI slotUri) {
        LOG.info("TerminalOutputSlot::connect " + slotUri);
        ForkJoinPool.commonPool().execute(() -> {
            try (SlotWriter writer = new SlotWriter(lzySlot, slotUri)) {
                writer.run();
            } catch (IOException | ExecutionException | InterruptedException e) {
                LOG.error("Failed to send slot " + lzySlot + " cause: " + e);
            }
        });
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
