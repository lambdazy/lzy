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

    public SlotApiClientImpl(LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient) {
        this.operationServiceClient = operationServiceClient;
    }

    @Override
    public void connect(Endpoint sender, Endpoint receiver, Duration timeout) {
        LOG.debug("[connect], sender={}, receiver={}", sender.uri(), receiver.uri());

        if (!receiver.isValid()) {
            LOG.error("[connect] failed, receiver is invalid, sender={}, receiver={}", sender.uri(), receiver.uri());
            // TODO
        }
        final var client = receiver.getSlotApiStub();

        LongRunning.Operation operation = client.connectSlot(LSA.ConnectSlotRequest.newBuilder()
            .setFrom(ProtoConverter.toProto(receiver.slot()))
            .setTo(ProtoConverter.toProto(sender.slot()))
            .build());

        LOG.debug("[connect] got operation, waiting response, sender={}, receiver={}", sender.uri(), receiver.uri());

        operation = waitOperationDone(operation, timeout);
        if (!operation.getDone()) {
            LOG.error("[connect] operation timeout, sender={}, receiver={}", sender.uri(), receiver.uri());
            throw new OperationTimeoutException();
            // TODO
        }
        if (operation.hasError()) {
            LOG.error("[connect] operation failed, sender={}, receiver={}", sender.uri(), receiver.uri());
            throw new RuntimeException(operation.getError().getMessage());
        }

        LOG.debug("[connect] done, sender={}, receiver={}", sender.uri(), receiver.uri());
    }

    @Override
    public void disconnect(Endpoint endpoint) {
        LOG.debug("[disconnect], endpoint={}", endpoint.uri());

        if (!endpoint.isValid()) {
            LOG.error("[connect] failed, endpoint is invalid, endpoint={}", endpoint.uri());
            // TODO
        }
        final var client = endpoint.getSlotApiStub();

        try {
            client.disconnectSlot(LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.slot()))
                .build());
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.debug("[disconnect] skipped, slot not found, endpoint={}", endpoint.uri());
                return;
            }
            LOG.debug("[disconnect] failed, endpoint={}", endpoint.uri());
            throw new RuntimeException(e.getMessage());
        }
        LOG.debug("[disconnect] done, endpoint={}", endpoint.uri());
    }

    @Override
    public void destroy(Endpoint endpoint) {
        LOG.debug("[destroy], endpoint={}", endpoint.uri());

        if (!endpoint.isValid()) {
            LOG.warn("[destroy] skipped, endpoint is invalid, endpoint={}", endpoint.uri());
            return;
        }

        final var client = endpoint.getSlotApiStub();
        try {
            client.destroySlot(LSA.DestroySlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.slot()))
                .build());
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.debug("[destroy] skipped, slot not found, endpoint={}", endpoint.uri());
                return;
            }
            LOG.debug("[destroy] failed, endpoint={}", endpoint.uri());
            throw new RuntimeException(e.getMessage());
        }
        LOG.debug("[destroy] done, endpoint={}", endpoint.uri());
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
