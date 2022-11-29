package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.slots.LSA;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;

@Singleton
public class SlotApiClientImpl implements SlotApiClient {

    private static final Logger LOG = LogManager.getLogger(SlotApiClientImpl.class);

    @Override
    public void connect(Endpoint sender, Endpoint receiver, Duration timeout) {
        LOG.debug("[connect], sender={}, receiver={}", sender.getUri(), receiver.getUri());

        SlotApiConnection receiverConnection = receiver.getSlotApiConnection();
        if (receiverConnection == null) {
            LOG.error("[connect] failed, receiver is invalid, sender={}, receiver={}",
                sender.getUri(), receiver.getUri());
            throw new RuntimeException("Invalid");
        }
        final var slotApiClient = receiverConnection.slotApiBlockingStub();
        final var operationApiClient = receiverConnection.operationApiBlockingStub();

        LongRunning.Operation operation = slotApiClient.connectSlot(LSA.ConnectSlotRequest.newBuilder()
            .setFrom(ProtoConverter.toProto(receiver.getSlot()))
            .setTo(ProtoConverter.toProto(sender.getSlot()))
            .build());

        LOG.debug("[connect] got operation, waiting response, sender={}, receiver={}",
            sender.getUri(), receiver.getUri());

        operation = awaitOperationDone(operationApiClient, operation.getId(), timeout);
        if (!operation.getDone()) {
            LOG.error("[connect] operation timeout, sender={}, receiver={}", sender.getUri(), receiver.getUri());
            throw new RuntimeException("Operation timeout");
        }
        if (operation.hasError()) {
            LOG.error("[connect] operation failed, sender={}, receiver={}", sender.getUri(), receiver.getUri());
            throw new RuntimeException(operation.getError().getMessage());
        }

        LOG.debug("[connect] done, sender={}, receiver={}", sender.getUri(), receiver.getUri());
    }

    @Override
    public void disconnect(Endpoint endpoint) {
        LOG.debug("[disconnect], endpoint={}", endpoint.getUri());

        SlotApiConnection connection = endpoint.getSlotApiConnection();
        if (connection == null) {
            LOG.warn("[disconnect] skipped, endpoint is invalid, endpoint={}", endpoint.getUri());
            return;
        }

        final var slotApiClient = connection.slotApiBlockingStub();
        try {
            slotApiClient.disconnectSlot(LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .build());
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.debug("[disconnect] skipped, slot not found, endpoint={}", endpoint.getUri());
                return;
            }
            LOG.debug("[disconnect] failed, endpoint={}", endpoint.getUri());
            throw new RuntimeException(e.getMessage());
        }
        LOG.debug("[disconnect] done, endpoint={}", endpoint.getUri());
    }

    @Override
    public void destroy(Endpoint endpoint) {
        LOG.debug("[destroy], endpoint={}", endpoint.getUri());

        SlotApiConnection connection = endpoint.getSlotApiConnection();
        if (connection == null) {
            LOG.warn("[destroy] skipped, endpoint is invalid, endpoint={}", endpoint.getUri());
            return;
        }

        final var slotApiClient = connection.slotApiBlockingStub();
        try {
            slotApiClient.destroySlot(LSA.DestroySlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(endpoint.getSlot()))
                .build());
        } catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.equals(e.getStatus())) {
                LOG.debug("[destroy] skipped, slot not found, endpoint={}", endpoint.getUri());
                return;
            }
            LOG.debug("[destroy] failed, endpoint={}", endpoint.getUri());
            throw new RuntimeException(e.getMessage());
        }
        LOG.debug("[destroy] done, endpoint={}", endpoint.getUri());
    }

}
