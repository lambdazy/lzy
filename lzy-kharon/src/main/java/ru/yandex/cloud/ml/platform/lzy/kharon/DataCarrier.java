package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataCarrier {
    private static final Logger LOG = LogManager.getLogger(DataCarrier.class);

    private final Map<String, StreamObserver<Servant.Message>> openDataConnections = new ConcurrentHashMap<>();

    public void openServantConnection(String slot, StreamObserver<Servant.Message> responseObserver) {
        openDataConnections.put(slot, responseObserver);
    }

    public StreamObserver<Kharon.SendSlotDataMessage> connectTerminalConnection(StreamObserver<Kharon.ReceivedDataStatus> receiver) {
        return new StreamObserver<>() {
            StreamObserver<Servant.Message> servantMessageStream;

            @Override
            public void onNext(Kharon.SendSlotDataMessage slotDataMessage) {
                switch (slotDataMessage.getWriteCommandCase()) {
                    case REQUEST: {
                        final Servant.SlotRequest request = slotDataMessage.getRequest();
                        final String slotName = request.getSlot();
                        servantMessageStream = openDataConnections.get(slotName);
                        break;
                    }
                    case MESSAGE: {
                        if (servantMessageStream == null) {
                            throw new IllegalStateException("Got message before request");
                        }
                        servantMessageStream.onNext(slotDataMessage.getMessage());
                        receiver.onNext(Kharon.ReceivedDataStatus.newBuilder()
                            .setStatus(Kharon.ReceivedDataStatus.Status.OK)
                            .build());
                        break;
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Error while LzyKharon::writeToInputSlot " + throwable);
                servantMessageStream.onError(throwable);
            }

            @Override
            public void onCompleted() {
                servantMessageStream.onCompleted();
            }
        };
    }
}
