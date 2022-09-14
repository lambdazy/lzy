package ai.lzy.servant.agents;

import static ai.lzy.model.deprecated.GrpcConverter.to;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.deprecated.Kharon.ReceivedDataStatus;
import ai.lzy.v1.deprecated.Kharon.SendSlotDataMessage;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TerminalSlotSender {

    private static final Logger LOG = LogManager.getLogger(TerminalSlotSender.class);
    private final LzyKharonGrpc.LzyKharonStub connectedSlotController;

    public TerminalSlotSender(LzyKharonGrpc.LzyKharonStub kharonStub) {
        connectedSlotController = kharonStub;
    }

    private static SendSlotDataMessage createChunkMessage(ByteString chunk) {
        return SendSlotDataMessage.newBuilder().setMessage(
            LzyFsApi.Message.newBuilder().setChunk(chunk).build()).build();
    }

    private static SendSlotDataMessage createEosMessage() {
        return SendSlotDataMessage.newBuilder().setMessage(
            LzyFsApi.Message.newBuilder().setControl(LzyFsApi.Message.Controls.EOS).build()).build();
    }

    public void connect(LzyOutputSlot lzySlot, SlotInstance toSlot) {
        LOG.info("TerminalOutputSlot::connect to " + toSlot);
        final SlotWriter writer = new SlotWriter(lzySlot, toSlot);
        writer.run();
    }

    private class SlotWriter {

        private final LzyOutputSlot lzySlot;
        private final SlotInstance toSlot;
        private final StreamObserver<SendSlotDataMessage> responseObserver;
        private long offset = 0;

        SlotWriter(LzyOutputSlot lzySlot, SlotInstance toSlot) {
            this.lzySlot = lzySlot;
            this.toSlot = toSlot;
            final StreamObserver<ReceivedDataStatus> statusReceiver = new StreamObserver<>() {
                @Override
                public void onNext(ReceivedDataStatus receivedDataStatus) {
                    LOG.info("Got response for slot " + lzySlot + " sending " + JsonUtils
                        .printRequest(receivedDataStatus));
                    offset = receivedDataStatus.getOffset();
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error(
                        "Exception while sending chunks from slot " + lzySlot + ": " + throwable);
                }

                @Override
                public void onCompleted() {
                    LOG.info("Sending chunks from slot " + lzySlot + " was finished");
                }
            };
            responseObserver = connectedSlotController.writeToInputSlot(statusReceiver);
        }

        public void run() {
            try {
                LOG.info("Starting sending bytes slot:: " + lzySlot);
                responseObserver.onNext(SendSlotDataMessage.newBuilder()
                    .setRequest(LzyFsApi.SlotRequest.newBuilder()
                        .setSlotInstance(ProtoConverter.toProto(toSlot))
                        .setOffset(offset)
                        .build())
                    .build());
                lzySlot.readFromPosition(offset)
                    .forEach(chunk -> responseObserver.onNext(createChunkMessage(chunk)));
                LOG.info("Completed sending bytes slot:: " + lzySlot);
                responseObserver.onNext(createEosMessage());
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Got exception while sending bytes slot:: " + lzySlot + " exception: " + e.getMessage(), e);
                responseObserver.onError(e);
            }
        }
    }

}
