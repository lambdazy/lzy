package ai.lzy.channelmanager.services;

import ai.lzy.channelmanager.access.IamAccessManager;
import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.channelmanager.operation.ChannelOperation;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.operation.ChannelOperationManager;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Objects;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelManagerDataSource storage;
    private final ChannelOperationManager channelOperationManager;
    private final IamAccessManager accessManager;
    private final ChannelOperationExecutor executor;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerService(ChannelDao channelDao,
                                 @Named("ChannelManagerOperationDao") OperationDao operationDao,
                                 ChannelOperationDao channelOperationDao, ChannelManagerDataSource storage,
                                 ChannelOperationManager channelOperationManager, GrainedLock lockManager,
                                 IamAccessManager accessManager, ChannelOperationExecutor executor)
    {
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.storage = storage;
        this.channelOperationManager = channelOperationManager;
        this.accessManager = accessManager;
        this.executor = executor;
        this.lockManager = lockManager;
    }

    @Override
    public void bind(LCMS.BindRequest request, StreamObserver<LongRunning.Operation> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("BindRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        final String slotUri = request.getSlotInstance().getSlotUri();
        final String channelId = request.getSlotInstance().getChannelId();
        String operationDescription = "Bind %s %s slot %s to channel %s"
            .formatted(request.getSlotOwner(), request.getSlotInstance().getSlot().getDirection(), slotUri, channelId);
        LOG.info(operationDescription + " started");


        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
            if (channel == null) {
                String errorMessage = "Channel " + channelId + " not found";
                LOG.error(operationDescription + " failed, {}", errorMessage);
                response.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        final var authenticationContext = AuthenticationContext.current();
        final String subjId = Objects.requireNonNull(authenticationContext).getSubject().id();

        final String userId = channel.getUserId();
        final String workflowName = channel.getWorkflowName();
        if (!accessManager.checkAccess(subjId, userId, workflowName, ChannelOperation.Type.BIND)) {
            LOG.error(operationDescription + "failed: PERMISSION DENIED to workflow {} of user {}",
                workflowName, userId);
            response.onError(Status.PERMISSION_DENIED.withDescription(
                "Don't have access to workflow " + channel.getWorkflowName()).asException());
            return;
        }

        Status preconditionsStatus = checkBindPreconditions(request, channelId, channel);
        if (!preconditionsStatus.isOk()) {
            LOG.error(operationDescription + " failed, {}", preconditionsStatus.getDescription());
            response.onError(preconditionsStatus.asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            Any.pack(LCMS.BindMetadata.getDefaultInstance()));

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newBindOperation(
            operation.id(), startedAt, deadline, channel.getExecutionId(), channelId, slotUri
        );

        final var slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final var slotOwner = fromProto(request.getSlotOwner());
        try {
            preconditionsStatus = withRetries(LOG, () -> {
                try (final var guard = lockManager.withLock(channelId);
                     final var tx = TransactionHandle.create(storage))
                {
                    final Status preconditionsActualStatus;

                    final Channel actualChannel = channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, tx);

                    preconditionsActualStatus = checkBindPreconditions(request, channelId, actualChannel);
                    if (!preconditionsActualStatus.isOk()) {
                        return preconditionsActualStatus;
                    }

                    channelDao.insertBindingEndpoint(slotInstance, slotOwner, tx);
                    channelOperationDao.create(channelOperation, tx);
                    operationDao.create(operation, tx);

                    tx.commit();

                    return preconditionsActualStatus;
                }
            });
            if (!preconditionsStatus.isOk()) {
                LOG.error(operationDescription + " failed, {}", preconditionsStatus.getDescription());
                response.onError(preconditionsStatus.asException());
                return;
            }
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        response.onCompleted();

        InjectedFailures.fail11();

        executor.submit(channelOperationManager.getAction(channelOperation));
    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LongRunning.Operation> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("UnbindRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        String operationDescription = "Unbind slot %s".formatted(request.getSlotUri());
        LOG.info(operationDescription + " started");

        final String slotUri = request.getSlotUri();
        final Endpoint endpoint;
        final Channel channel;
        try {
            endpoint = withRetries(LOG, () -> channelDao.findEndpoint(slotUri, null));
            if (endpoint == null) {
                String errorMessage = "Endpoint " + slotUri + " not found";
                LOG.error(operationDescription + " failed, {}", errorMessage);
                response.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            channel = withRetries(LOG, () -> channelDao.findChannel(endpoint.getChannelId(), null));
            if (channel == null) {
                String errorMessage = "Channel " + endpoint.getChannelId() + " not found";
                LOG.error(operationDescription + " failed, {}", errorMessage);
                response.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        final var authenticationContext = AuthenticationContext.current();
        final String subjId = Objects.requireNonNull(authenticationContext).getSubject().id();

        final String channelId = channel.getId();
        final String userId = channel.getUserId();
        final String workflowName = channel.getWorkflowName();
        if (!accessManager.checkAccess(subjId, userId, workflowName, ChannelOperation.Type.UNBIND)) {
            LOG.error(operationDescription + "failed: PERMISSION DENIED to workflow {} of user {}",
                workflowName, userId);
            response.onError(Status.PERMISSION_DENIED.withDescription(
                "Don't have access to workflow " + channel.getWorkflowName()).asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            Any.pack(LCMS.UnbindMetadata.getDefaultInstance()));

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newUnbindOperation(
            operation.id(), startedAt, deadline, channel.getExecutionId(), channelId, slotUri
        );

        try {
            final Status preconditionsStatus = withRetries(LOG, () -> {
                try (final var guard = lockManager.withLock(channelId);
                     final var tx = TransactionHandle.create(storage))
                {
                    final Status preconditionsActualStatus;

                    final Channel actualChannel = channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, tx);

                    preconditionsActualStatus = checkUnbindPreconditions(request, channelId, actualChannel);
                    if (!preconditionsActualStatus.isOk()) {
                        return preconditionsActualStatus;
                    }

                    channelDao.markEndpointUnbinding(slotUri, tx);
                    channelOperationDao.create(channelOperation, tx);
                    operationDao.create(operation, tx);

                    tx.commit();

                    return preconditionsActualStatus;
                }
            });
            if (!preconditionsStatus.isOk()) {
                LOG.error(operationDescription + " failed, {}", preconditionsStatus.getDescription());
                response.onError(preconditionsStatus.asException());
                return;
            }
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        response.onCompleted();

        InjectedFailures.fail12();

        executor.submit(channelOperationManager.getAction(channelOperation));
    }

    private Status checkBindPreconditions(LCMS.BindRequest request, String channelId, Channel channel) {
        if (channel == null) {
            return Status.NOT_FOUND.withDescription("Channel " + channelId + " not found");
        }

        switch (request.getSlotOwner()) {
            case PORTAL -> {
                Endpoint activeSenderPortalEndpoint = channel.getActiveSenders().portalEndpoint();
                Endpoint activeReceiverPortalEndpoint = channel.getActiveReceivers().portalEndpoint();
                if (activeSenderPortalEndpoint != null || activeReceiverPortalEndpoint != null) {
                    String errorMessage = "PORTAL endpoint already bound to channel";
                    return Status.FAILED_PRECONDITION.withDescription(errorMessage);
                }
            }
            case WORKER -> {
                Endpoint activeSenderWorkerEndpoint = channel.getActiveSenders().workerEndpoint();
                LMS.Slot.Direction newEndpointDirection = request.getSlotInstance().getSlot().getDirection();
                if (newEndpointDirection == LMS.Slot.Direction.OUTPUT && activeSenderWorkerEndpoint != null) {
                    String errorMessage = "WORKER endpoint already bound as input slot to channel";
                    return Status.FAILED_PRECONDITION.withDescription(errorMessage);
                }
            }
        }

        final Endpoint activeEndpoint = channel.getEndpoints().stream()
            .filter(e -> e.getUri().toString().equals(request.getSlotInstance().getSlotUri()))
            .filter(Endpoint::isActive)
            .findFirst().orElse(null);

        if (activeEndpoint != null) {
            String errorMessage = "Endpoint " + request.getSlotInstance().getSlotUri() + " already exists";
            return Status.ALREADY_EXISTS.withDescription(errorMessage);
        }


        return Status.OK;
    }

    private Status checkUnbindPreconditions(LCMS.UnbindRequest request, String channelId, Channel channel) {
        if (channel == null) {
            return Status.NOT_FOUND.withDescription("Channel " + channelId + " not found");
        }

        final Endpoint activeEndpoint = channel.getEndpoints().stream()
            .filter(e -> e.getUri().toString().equals(request.getSlotUri()))
            .filter(Endpoint::isActive)
            .findFirst().orElse(null);

        if (activeEndpoint == null) {
            String errorMessage = "Endpoint " + request.getSlotUri() + " not found";
            return Status.NOT_FOUND.withDescription(errorMessage);
        }

        return Status.OK;
    }

    private Endpoint.SlotOwner fromProto(LCMS.BindRequest.SlotOwner origin) {
        return Endpoint.SlotOwner.valueOf(origin.name());
    }

}
