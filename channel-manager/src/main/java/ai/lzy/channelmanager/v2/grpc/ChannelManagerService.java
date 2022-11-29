package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.channelmanager.v2.model.EndpointFactory;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.model.slot.Slot;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelStorage channelStorage;
    private final OperationDao operationStorage;
    private final ExecutorService longrunningExecutor;
    private final ChannelController channelController;
    private final GrainedLock lockManager;
    private final EndpointFactory endpointFactory;

    @Inject
    public ChannelManagerService(ChannelStorage channelStorage, OperationDao operationStorage,
                                 ChannelController channelController, GrainedLock lockManager,
                                 EndpointFactory endpointFactory) {
        this.channelStorage = channelStorage;
        this.operationStorage = operationStorage;
        this.channelController = channelController;
        this.lockManager = lockManager;
        this.endpointFactory = endpointFactory;

        this.longrunningExecutor = new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "lr-channel-manager-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
    }

    @Override
    public void bind(LCMS.BindRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Bind slot to channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        final String slotUri = request.getSlotInstance().getSlotUri();
        final String channelId = request.getSlotInstance().getChannelId();
        String operationDescription = "Bind %s slot %s to channel %s"
            .formatted(request.getSlotOwner(), slotUri, channelId);
        LOG.info(operationDescription);

        final Endpoint endpoint = endpointFactory.createEndpoint(
            request.getSlotInstance(), request.getSlotOwner(), Endpoint.LifeStatus.BINDING);
        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel not found";
                LOG.error(operationDescription + " failed, {}", errorMessage);
                responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            if (endpoint.getSlotOwner() == Endpoint.SlotOwner.PORTAL) {
                if (channel.getActiveSenders().portalEndpoint() != null
                    || channel.getActiveReceivers().portalEndpoint() != null)
                {
                    String errorMessage = "Portal endpoint already bound to channel";
                    LOG.error(operationDescription + " failed, {}", errorMessage);
                    responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(errorMessage).asException());
                    return;
                }
            }
            if (endpoint.getSlotOwner() == Endpoint.SlotOwner.WORKER
                && endpoint.getSlotDirection() == Slot.Direction.INPUT)
            {
                if (channel.getActiveSenders().workerEndpoint() != null) {
                    String errorMessage = "Worker endpoint already bound as input slot to channel";
                    LOG.error(operationDescription + " failed, {}", errorMessage);
                    responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(errorMessage).asException());
                    return;
                }
            }

            withRetries(LOG, () -> channelStorage.insertBindingEndpoint(endpoint, null));
        } catch (AlreadyExistsException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage());
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asException());
            return;
        } catch (NotFoundException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage());
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = new Operation("ChannelManager", operationDescription,
            Any.pack(LCMS.BindMetadata.getDefaultInstance()));
        try {
            withRetries(LOG, () -> operationStorage.create(operation, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        responseObserver.onCompleted();

        longrunningExecutor.submit(() -> {
            try {
                LOG.info(operationDescription + " responded, async operation started, operationId={}", operation.id());

                channelController.bind(endpoint);

                try {
                    withRetries(LOG, () -> operationStorage.updateResponse(operation.id(),
                        Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray(), null));
                } catch (Exception e) {
                    LOG.error("Cannot update operation", e);
                    return;
                }

                LOG.info(operationDescription + " responded, async operation finished, operationId={}", operation.id());
            } catch (CancellingChannelGraphStateException e) {
                String errorMessage = operationDescription + " async operation " + operation.id()
                                      + " cancelled according to the graph state: " + e.getMessage();
                LOG.error(errorMessage);
                failOperation(operation.id(), Status.CANCELLED, errorMessage);
            } catch (Exception e) {
                String errorMessage = operationDescription + " async operation " + operation.id()
                                      + " failed: " + e.getMessage();
                LOG.error(errorMessage);
                failOperation(operation.id(), errorMessage);
                // TODO
            }
        });
    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Unbind slot failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        String operationDescription = "Unbind slot %s".formatted(request.getSlotUri());
        LOG.info(operationDescription);

        final String slotUri = request.getSlotUri();
        final Endpoint endpoint;
        try {
            endpoint = channelStorage.getEndpoint(slotUri, null);
        } catch (NotFoundException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        try (final var guard = lockManager.withLock(endpoint.getChannelId())) {
            withRetries(LOG, () -> channelStorage.markEndpointUnbinding(slotUri, null));
        }  catch (NotFoundException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = new Operation("ChannelManager", operationDescription,
            Any.pack(LCMS.UnbindMetadata.getDefaultInstance()));
        try {
            withRetries(LOG, () -> operationStorage.create(operation, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        responseObserver.onCompleted();

        longrunningExecutor.submit(() -> {
            try {
                LOG.info(operationDescription + " responded, async operation started, operationId={}", operation.id());

                switch (endpoint.getSlotDirection()) {
                    case OUTPUT /* SENDER */ -> channelController.unbindSender(endpoint);
                    case INPUT /* RECEIVER */ -> channelController.unbindReceiver(endpoint);
                }

                try {
                    withRetries(LOG, () -> operationStorage.updateResponse(operation.id(),
                        Any.pack(LCMS.BindResponse.getDefaultInstance()).toByteArray(), null));
                } catch (Exception e) {
                    LOG.error("Cannot update operation", e);
                    return;
                }

                LOG.info(operationDescription + " responded, async operation finished, operationId={}", operation.id());
            } catch (CancellingChannelGraphStateException e) {
                String errorMessage = operationDescription + " async operation " + operation.id()
                                      + " cancelled according to the graph state: " + e.getMessage();
                LOG.error(errorMessage);
                failOperation(operation.id(), Status.CANCELLED, errorMessage);
            } catch (Exception e) {
                String errorMessage = operationDescription + " async operation " + operation.id()
                                      + " failed: " + e.getMessage();
                LOG.error(errorMessage);
                failOperation(operation.id(), errorMessage);
                // TODO
            }
        });

    }

    private void failOperation(String operationId, String message) {
        failOperation(operationId, Status.INTERNAL, message);
    }

    private void failOperation(String operationId, Status opStatus, String message) {
        var status = com.google.rpc.Status.newBuilder()
            .setCode(opStatus.getCode().value())
            .setMessage(message)
            .build();
        try {
            var op = withRetries(LOG, () -> operationStorage.updateError(operationId, status.toByteArray(), null));
            if (op == null) {
                LOG.error("Cannot fail operation {} with reason {}: operation not found",
                    operationId, message);
            }
        } catch (Exception ex) {
            LOG.error("Cannot fail operation {} with reason {}: {}",
                operationId, message, ex.getMessage(), ex);
        }
    }
}
