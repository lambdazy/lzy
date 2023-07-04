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
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.LCM;
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

import static ai.lzy.channelmanager.grpc.ProtoConverter.toProto;
import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
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
        String operationDescription = "Bind %s %s slot (slotUri: %s, channelId: %s)"
            .formatted(request.getSlotOwner(), request.getSlotInstance().getSlot().getDirection(), slotUri, channelId);
        LOG.info(operationDescription + " started");

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, response, LOG)) {
            return;
        }

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
        if (!checkAccess(subjId, userId, workflowName, ChannelOperation.Type.BIND)) {
            LOG.error(operationDescription + "failed: PERMISSION DENIED; workflowName: {}, userId: {}",
                workflowName, userId);
            response.onError(Status.PERMISSION_DENIED.withDescription(
                "Don't have access to workflow " + channel.getWorkflowName()).asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            idempotencyKey, Any.pack(LCMS.BindMetadata.getDefaultInstance()));

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newBindOperation(
            operation.id(), startedAt, deadline, workflowName, channel.getExecutionId(), channelId, slotUri
        );

        final var slotInstance = ProtoConverter.fromProto(request.getSlotInstance());
        final var slotOwner = fromProto(request.getSlotOwner());
        try {
            Status preconditionsStatus = withRetries(LOG, () -> {
                try (final var guard = lockManager.withLock(channelId);
                     final var tx = TransactionHandle.create(storage))
                {
                    operationDao.create(operation, tx);

                    final Status preconditionsActualStatus;

                    final Channel actualChannel = channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, tx);

                    preconditionsActualStatus = checkBindPreconditions(request, channelId, actualChannel);
                    if (!preconditionsActualStatus.isOk()) {
                        return preconditionsActualStatus;
                    }

                    channelDao.insertBindingEndpoint(slotInstance, slotOwner, tx);
                    channelOperationDao.create(channelOperation, tx);

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
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
            {
                return;
            }

            LOG.error(operationDescription + " failed, cannot create operation (operationId: {}), got exception: {}",
                operation.id(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled; operationId: {}", operation.id());
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

        String operationDescription = "Unbind slot (slotUri: %s)".formatted(request.getSlotUri());
        LOG.info(operationDescription + " started");

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, response, LOG)) {
            return;
        }

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
        if (!checkAccess(subjId, userId, workflowName, ChannelOperation.Type.UNBIND)) {
            LOG.error(operationDescription + "failed: PERMISSION DENIED; workflowName: {}, userId: {}",
                workflowName, userId);
            response.onError(Status.PERMISSION_DENIED.withDescription(
                "Don't have access to workflow " + channel.getWorkflowName()).asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            idempotencyKey, Any.pack(LCMS.UnbindMetadata.getDefaultInstance()));

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newUnbindOperation(
            operation.id(), startedAt, deadline, workflowName, channel.getExecutionId(), channelId, slotUri
        );

        try {
            final Status preconditionsStatus = withRetries(LOG, () -> {
                try (final var guard = lockManager.withLock(channelId);
                     final var tx = TransactionHandle.create(storage))
                {
                    operationDao.create(operation, tx);

                    final Status preconditionsActualStatus;

                    final Channel actualChannel = channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, tx);

                    preconditionsActualStatus = checkUnbindPreconditions(request, channelId, actualChannel);
                    if (!preconditionsActualStatus.isOk()) {
                        return preconditionsActualStatus;
                    }

                    channelDao.markEndpointUnbinding(slotUri, tx);
                    channelOperationDao.create(channelOperation, tx);

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
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
            {
                return;
            }

            LOG.error(operationDescription + " failed, cannot create operation (operationId: {}), got exception: {}",
                operation.id(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled; operationId: {}", operation.id());
        response.onCompleted();

        InjectedFailures.fail12();

        executor.submit(channelOperationManager.getAction(channelOperation));
    }

    @Override
    public void getChannelsStatus(LCMS.GetChannelsStatusRequest request,
                                  StreamObserver<LCMS.GetChannelsStatusResponse> response)
    {
        var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("GetChannelStatusRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        var authenticationContext = AuthenticationContext.current();
        var subjId = Objects.requireNonNull(authenticationContext).getSubject().id();

        var responseBuilder = LCMS.GetChannelsStatusResponse.newBuilder();

        for (var channelId : request.getChannelIdsList()) {
            final Channel channel;
            try {
                channel = withRetries(LOG, () -> channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, null));
            } catch (Exception e) {
                LOG.error("Get channel status (executionId: {}, channelId: {}) failed, got exception: {}",
                    request.getExecutionId(), channelId, e.getMessage(), e);
                response.onError(Status.INTERNAL
                    .withDescription("Cannot load channel %s: %s".formatted(channelId, e.getMessage())).asException());
                return;
            }

            if (channel == null) {
                LOG.error("Get channel status (executionId: {}, channelId: {}) failed, channel not found",
                    request.getExecutionId(), channelId);
                responseBuilder.addChannels(
                    LCM.Channel.newBuilder()
                        .setChannelId(channelId)
                        // .setSpec()  // no spec - no (alive) channel
                        .build());
                continue;
            }

            if (!channel.getExecutionId().equals(request.getExecutionId())) {
                LOG.error("Get channel status (executionId: {}, channelId: {}) failed: " +
                        "requested executionId differs from channel's executionId: {}",
                    request.getExecutionId(), channelId, channel.getExecutionId());
                response.onError(Status.INVALID_ARGUMENT.withDescription("Hack attempt fails").asException());
                return;
            }

            final String userId = channel.getUserId();
            final String workflowName = channel.getWorkflowName();
            if (!checkAccess(subjId, userId, workflowName, ChannelOperation.Type.BIND)) {
                LOG.error("Get channel status (executionId: {}, channelId: {}) failed: PERMISSION DENIED; " +
                        "workflowName: {}, userId: {}", request.getExecutionId(), channelId, workflowName, userId);
                response.onError(Status.PERMISSION_DENIED.withDescription(
                    "Don't have access to workflow " + channel.getWorkflowName()).asException());
                return;
            }

            responseBuilder.addChannels(toProto(channel));
        }

        response.onNext(responseBuilder.build());
        response.onCompleted();
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

    private boolean checkAccess(String subjId, String userId, String workflowName, ChannelOperation.Type opType) {
        // TODO: retries
        final var permission = switch (opType) {
            case BIND, UNBIND -> AuthPermission.WORKFLOW_RUN;
            case DESTROY -> AuthPermission.WORKFLOW_STOP;
        };

        return accessManager.checkAccess(subjId, userId, workflowName, permission);
    }
}
