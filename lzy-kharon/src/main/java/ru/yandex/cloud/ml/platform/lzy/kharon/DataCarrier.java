package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class DataCarrier {
    private static final Logger LOG = LogManager.getLogger(DataCarrier.class);

    private final Map<String, StreamObserver<Servant.Message>> openDataConnections = new ConcurrentHashMap<>();

    public void openServantConnection(String slotUri, StreamObserver<Servant.Message> responseObserver) {
        LOG.info("DataCarrier::openServantConnection for slotUri " + slotUri);
        openDataConnections.put(slotUri, responseObserver);
    }

    public StreamObserver<Kharon.SendSlotDataMessage> connectTerminalConnection(StreamObserver<Kharon.ReceivedDataStatus> receiver) {
        LOG.info("DataCarrier::connectTerminalConnection");
        return new StreamObserver<>() {
            final CompletableFuture<StreamObserver<Servant.Message>> servantMessageStream = new CompletableFuture<>();

            @Override
            public void onNext(Kharon.SendSlotDataMessage slotDataMessage) {
                LOG.info("DataCarrier::onNext " + JsonUtils.printRequest(slotDataMessage));
                switch (slotDataMessage.getWriteCommandCase()) {
                    case REQUEST: {
                        final Servant.SlotRequest request = slotDataMessage.getRequest();
                        final String slotUri = request.getSlotUri();
                        servantMessageStream.complete(openDataConnections.get(slotUri));
                        break;
                    }
                    case MESSAGE: {
                        getServantMessageStream().onNext(slotDataMessage.getMessage());
                        receiver.onNext(Kharon.ReceivedDataStatus.newBuilder()
                            .setStatus(Kharon.ReceivedDataStatus.Status.OK)
                            .build());
                        break;
                    }
                }
            }

            private StreamObserver<Servant.Message> getServantMessageStream() {
                try {
                    return servantMessageStream.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Error while LzyKharon::writeToInputSlot " + throwable);
                getServantMessageStream().onError(throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("DataCarrier:: sending completed");
//                getServantMessageStream().onCompleted();
            }
        };
    }
}
