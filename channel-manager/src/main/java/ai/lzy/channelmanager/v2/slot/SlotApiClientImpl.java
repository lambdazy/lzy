package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.util.grpc.ProtoConverter.fromProto;

public class SlotApiClientImpl implements SlotApiClient {

    private static final Logger LOG = LogManager.getLogger(SlotApiClientImpl.class);

    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient;

    @Override
    public void connect(Endpoint sender, Endpoint receiver, Duration timeout) {
        final var client = receiver.getSlotApiStub();

        LongRunning.Operation operation = client.connectSlot(LSA.ConnectSlotRequest.newBuilder()
            .setFrom(ProtoConverter.toProto(receiver.slot()))
            .setTo(ProtoConverter.toProto(sender.slot()))
            .build());

        operation = waitOperationDone(operation, timeout);
        if (!operation.getDone()) {
            LOG.error("C", sender.uri(), receiver.uri());
            throw new OperationTimeoutException();
        }
        if (operation.hasError()) {
            throw new RuntimeException(operation.getError().getMessage());
        }

    }

    @Override
    public void disconnect(Endpoint endpoint) {
        final var client = endpoint.getSlotApiStub();

        try {
            client.disconnectSlot(LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.slot()))
                .build());
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.warn("Slot not found, skipping disconnect");
            }
        }
    }

    @Override
    public void destroy(Endpoint endpoint) {

    }

    private LongRunning.Operation waitOperationDone(LongRunning.Operation operation, Duration timeout) {
        if (operation.getDone()) {
            return operation;
        }
        Instant waitingLimit = fromProto(operation.getCreatedAt()).plus(timeout);
        while (Instant.now().isBefore(waitingLimit)) {
            operation = operationServiceClient.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(operation.getId()).build());
            if (operation.getDone()) {
                return operation;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
        }
        return operation;
    }

}
