package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotRequest;

public class DataCarrier {

    private static final Logger LOG = LogManager.getLogger(DataCarrier.class);

    private final Map<URI, StreamObserver<Servant.Message>> openDataConnections = new ConcurrentHashMap<>();

    public void openServantConnection(URI slotUri, StreamObserver<Servant.Message> responseObserver) {
        LOG.info("DataCarrier::openServantConnection for slotUri " + slotUri);
        openDataConnections.put(slotUri, responseObserver);
    }

    public StreamObserver<Kharon.SendSlotDataMessage> connectTerminalConnection(
        StreamObserver<Kharon.ReceivedDataStatus> receiver) {
        LOG.info("DataCarrier::connectTerminalConnection");
        return new StreamObserver<>() {
            final CompletableFuture<StreamObserver<Servant.Message>> servantMessageStream = new CompletableFuture<>();

            @Override
            public void onNext(Kharon.SendSlotDataMessage slotDataMessage) {
                LOG.info("DataCarrier::onNext " + JsonUtils.printRequest(slotDataMessage));
                switch (slotDataMessage.getWriteCommandCase()) {
                    case REQUEST: {
                        final SlotRequest request = slotDataMessage.getRequest();
                        final URI slotUri = URI.create(request.getSlotUri());
                        StreamObserver<Servant.Message> messageStream = openDataConnections.get(slotUri);
                        if (messageStream == null) {
                            StatusRuntimeException exception = Status.RESOURCE_EXHAUSTED
                                .withDescription("writeToInputStream must be called only after openOutputSlot")
                                .asRuntimeException();
                            servantMessageStream.completeExceptionally(exception);
                            throw exception;
                        }
                        servantMessageStream.complete(messageStream);
                        break;
                    }
                    case MESSAGE: {
                        getServantMessageStream().onNext(slotDataMessage.getMessage());
                        break;
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + slotDataMessage.getWriteCommandCase());
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
                receiver.onNext(
                    Kharon.ReceivedDataStatus.newBuilder()
                        .setStatus(Kharon.ReceivedDataStatus.Status.OK)
                        .build()
                );
                receiver.onCompleted();
                getServantMessageStream().onCompleted();
            }
        };
    }
}
