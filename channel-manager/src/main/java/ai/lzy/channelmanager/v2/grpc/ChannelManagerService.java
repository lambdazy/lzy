package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.model.slot.Slot;
import ai.lzy.util.grpc.ContextAwareTask;
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

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelStorage channelStorage;
    private final OperationDao operationStorage;
    private final ExecutorService longrunningExecutor;
    private final ChannelController channelController;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerService(GrainedLock lockManager, ChannelStorage channelStorage,
                                 OperationDao operationDao, ChannelController channelController)
    {
        this.channelStorage = channelStorage;
        this.operationStorage = operationDao;
        this.channelController = channelController;
        this.lockManager = lockManager;

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

        final Endpoint endpoint = Endpoint.fromProto(request.getSlotInstance(), request.getSlotOwner());
        try (final var guard = lockManager.withLock(channelId)) {
            Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                // not found exception
                return;
            }

            if (endpoint.slotOwner() == Endpoint.SlotOwner.PORTAL) {
                if (channel.existedSenders().portalEndpoint() != null || channel.existedReceivers().portalEndpoint() != null) {
                    // invalid argument error
                }
            }
            if (endpoint.slotOwner() == Endpoint.SlotOwner.WORKER && endpoint.slotDirection() == Slot.Direction.INPUT) {
                if (channel.existedSenders().workerEndpoint() != null) {
                    // invalid argument error
                }
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.insertBindingEndpoint(endpoint, null));
        } catch (AlreadyExistsException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage());
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asException());
            return;
        } catch (NotFoundException e) {
            LOG.error("Bind {} slot={} to channel={} failed, "
                      + "{}", request.getSlotOwner(), slotUri, channelId, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Bind {} slot={} to channel={} failed, "
                      + "got exception: {}", request.getSlotOwner(), slotUri, channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation bindOperation = new Operation("ChannelManager", operationDescription, Any.pack(LCMS.BindMetadata.getDefaultInstance()));
        try {
            operationStorage.create(bindOperation, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(bindOperation.toProto());
        responseObserver.onCompleted();
        LOG.info(operationDescription + " responded, async operation started");

        longrunningExecutor.submit(new ContextAwareTask() {
            @Override
            protected void execute() {
                try {
                    channelController.bind(endpoint);
                } catch (CancellingChannelGraphStateException e) {
                    LOG.warn("[executeBind] operation {} cancelled, " + e.getMessage());
                    bindOperation.setError(Status.CANCELLED);
                    operationStorage.update(bindOperation);
                } catch (Exception e) {
                    LOG.error("[executeBind] operation {} failed, " + e.getMessage());
                    // error op
                }

                bindOperation.setResponse(Any.pack(LCMS.BindResponse.getDefaultInstance()));
                operationStorage.update(bindOperation);
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
            LOG.error("Unbind slot {} failed, {}",  slotUri, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Unbind slot {} failed, got exception: {}", slotUri, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        try (final var guard = lockManager.withLock(endpoint.channelId())) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointUnbinding(slotUri, null));
        }  catch (NotFoundException e) {
            LOG.error("Unbind slot {} failed, {}",  slotUri, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Unbind slot {} failed, got exception: {}", slotUri, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = new Operation("ChannelManager", operationDescription, Any.pack(LCMS.UnbindMetadata.getDefaultInstance()));
        try {
            operationStorage.create(operation, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(operation.toProto());
        responseObserver.onCompleted();
        LOG.info(operationDescription + " responded, async operation started");

        longrunningExecutor.submit(new ContextAwareTask() {
            @Override
            protected void execute() {
                try {
                    switch (endpoint.slotDirection()) {
                        case OUTPUT /* SENDER */ -> channelController.unbindSender(endpoint);
                        case INPUT /* RECEIVER */ -> channelController.unbindReceiver(endpoint);
                    }
                } catch (Exception e) {

                }
            }
        });

    }
}
