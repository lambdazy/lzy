package ai.lzy.channelmanager.services;

import ai.lzy.channelmanager.dao.ChannelDao;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.dao.ChannelOperationDao;
import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.model.channel.Channel;
import ai.lzy.channelmanager.operation.ChannelOperation;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.operation.ChannelOperationManager;
import ai.lzy.channelmanager.test.InjectedFailures;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
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
import java.util.List;
import java.util.stream.Collectors;

import static ai.lzy.channelmanager.grpc.ProtoConverter.toProto;
import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerPrivateService extends LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerPrivateService.class);

    private final ChannelDao channelDao;
    private final OperationDao operationDao;
    private final ChannelOperationDao channelOperationDao;
    private final ChannelManagerDataSource storage;
    private final ChannelOperationManager channelOperationManager;
    private final ChannelOperationExecutor executor;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerPrivateService(ChannelDao channelDao,
                                        @Named("ChannelManagerOperationDao") OperationDao operationDao,
                                        ChannelOperationDao channelOperationDao, ChannelManagerDataSource storage,
                                        ChannelOperationManager channelOperationManager, GrainedLock lockManager,
                                        ChannelOperationExecutor executor)
    {
        this.channelDao = channelDao;
        this.operationDao = operationDao;
        this.channelOperationDao = channelOperationDao;
        this.storage = storage;
        this.channelOperationManager = channelOperationManager;
        this.executor = executor;
        this.lockManager = lockManager;
    }

    @Override
    public void create(LCMPS.ChannelCreateRequest request, StreamObserver<LCMPS.ChannelCreateResponse> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("ChannelCreateRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        final String channelName = request.getChannelSpec().getChannelName();
        String operationDescription = "Create channel " + channelName;
        LOG.info(operationDescription + " started");

        final String userId = request.getUserId();
        final String workflowName = request.getWorkflowName();
        final String executionId = request.getExecutionId();
        final LCM.ChannelSpec channelSpec = request.getChannelSpec();
        final String channelId = "channel-" + executionId.replaceAll("[^a-zA-z0-9-]+", "-") + "-" + channelName;

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(LOG, () -> channelDao.insertChannel(
                channelId, executionId, workflowName, userId, fromProto(channelSpec), null));
        } catch (AlreadyExistsException e) {
            LOG.error(operationDescription + " failed, channel already exists");
            response.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        response.onNext(LCMPS.ChannelCreateResponse.newBuilder().setChannelId(channelId).build());
        LOG.info(operationDescription + " done, channelId={}", channelId);
        response.onCompleted();
    }

    @Override
    public void destroy(LCMPS.ChannelDestroyRequest request, StreamObserver<LongRunning.Operation> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("ChannelDestroyRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        final String channelId = request.getChannelId();
        String operationDescription = "Destroy channel " + channelId;
        LOG.info(operationDescription + " started");

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, response, LOG)) {
            return;
        }

        final Channel channel;
        try {
            channel = withRetries(LOG, () -> channelDao.findChannel(channelId, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            idempotencyKey, Any.pack(LCMPS.ChannelDestroyMetadata.getDefaultInstance()));

        if (channel == null) {
            LOG.warn(operationDescription + " skipped, channel not found");

            operation.setResponse(Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()));
            try {
                withRetries(LOG, () -> operationDao.create(operation, null));
            } catch (Exception e) {
                if (idempotencyKey != null &&
                    handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
                {
                    return;
                }

                LOG.error(operationDescription + " failed, cannot create operation, "
                          + "got exception: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
                return;
            }

            response.onNext(operation.toProto());
            response.onCompleted();
            return;
        }

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newDestroyOperation(
            operation.id(), startedAt, deadline, channel.getExecutionId(), List.of(channelId)
        );

        try {
            withRetries(LOG, () -> {
                try (final var guard = lockManager.withLock(channelId);
                     final var tx = TransactionHandle.create(storage))
                {
                    operationDao.create(operation, tx);
                    channelDao.markChannelDestroying(channelId, tx);
                    channelOperationDao.create(channelOperation, tx);

                    tx.commit();
                }
            });
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
            {
                return;
            }

            LOG.error(operationDescription + " failed, cannot create operation, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        response.onCompleted();

        InjectedFailures.fail13();

        executor.submit(channelOperationManager.getAction(channelOperation));
    }

    @Override
    public void destroyAll(LCMPS.ChannelDestroyAllRequest request, StreamObserver<LongRunning.Operation> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("ChannelDestroyAllRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        final String executionId = request.getExecutionId();
        String operationDescription = "Destroying all channels for execution " + executionId;
        LOG.info(operationDescription + " started");

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, response, LOG)) {
            return;
        }

        final List<String> channelsToDestroy;
        try {
            final List<Channel> channels = withRetries(LOG, () -> channelDao.listChannels(executionId));
            channelsToDestroy = channels.stream().map(Channel::getId).collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = Operation.create("ChannelManager", operationDescription, /* deadline */ null,
            idempotencyKey, Any.pack(LCMPS.ChannelDestroyAllMetadata.getDefaultInstance()));

        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(30);
        final ChannelOperation channelOperation = channelOperationManager.newDestroyOperation(
            operation.id(), startedAt, deadline, executionId, channelsToDestroy
        );

        try {
            withRetries(LOG, () -> {
                try (final var tx = TransactionHandle.create(storage)) {
                    channelOperationDao.create(channelOperation, tx);
                    operationDao.create(operation, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, e, operationDao, response, LOG))
            {
                return;
            }

            LOG.error(operationDescription + " failed, cannot create operation, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            return;
        }

        for (final var channelId : channelsToDestroy) {
            try (final var guard = lockManager.withLock(channelId)) {
                withRetries(LOG, () -> channelDao.markChannelDestroying(channelId, null));
            } catch (Exception e) {
                LOG.warn("Failed to mark channel {} destroying, will do it later, got exception: {}",
                    channelId, e.getMessage());
            }
        }

        // TODO test on failure after adding idempotency token

        response.onNext(operation.toProto());
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());
        response.onCompleted();

        InjectedFailures.fail14();

        executor.submit(channelOperationManager.getAction(channelOperation));
    }

    @Override
    public void status(LCMPS.ChannelStatusRequest request, StreamObserver<LCMPS.ChannelStatusResponse> response) {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("ChannelStatusRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        LOG.info("Get status for channel {}", request.getChannelId());

        final String channelId = request.getChannelId();
        final Channel channel;
        try {
            channel = channelDao.findChannel(channelId, Channel.LifeStatus.ALIVE, null);
        } catch (Exception e) {
            LOG.error("Get status for channel {} failed, got exception: {}", channelId, e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        if (channel == null) {
            LOG.error("Get status for channel {} failed, channel not found", request.getChannelId());
            response.onError(Status.NOT_FOUND.asException());
            return;
        }

        response.onNext(createChannelStatusResponse(channel));
        LOG.info("Get status for channel {} done", channelId);
        response.onCompleted();
    }

    @Override
    public void statusAll(LCMPS.ChannelStatusAllRequest request,
                          StreamObserver<LCMPS.ChannelStatusAllResponse> response)
    {
        final var validationResult = ProtoValidator.validate(request);
        if (!validationResult.isOk()) {
            LOG.error("ChannelStatusAllRequest failed: {}", validationResult.description());
            response.onError(Status.INVALID_ARGUMENT.withDescription(validationResult.description()).asException());
            return;
        }

        LOG.info("Get status for channels of execution {}", request.getExecutionId());

        final String executionId = request.getExecutionId();
        List<Channel> aliveChannels;
        try {
            aliveChannels = withRetries(LOG, () ->
                channelDao.listChannels(executionId, Channel.LifeStatus.ALIVE));
        } catch (Exception e) {
            LOG.error("Get status for channels of execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        response.onNext(createChannelStatusAllResponse(aliveChannels));
        LOG.info("Get status for channels of execution {} done", executionId);
        response.onCompleted();
    }

    private LCMPS.ChannelStatusResponse createChannelStatusResponse(Channel channel) {
        return LCMPS.ChannelStatusResponse.newBuilder()
            .setStatus(LCMPS.ChannelStatus.newBuilder()
                .setChannel(toProto(channel))
                .build())
            .build();
    }

    private LCMPS.ChannelStatusAllResponse createChannelStatusAllResponse(List<Channel> channels) {
        return LCMPS.ChannelStatusAllResponse.newBuilder()
            .addAllStatuses(channels.stream()
                .map(channel -> LCMPS.ChannelStatus.newBuilder()
                    .setChannel(toProto(channel))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private static Channel.Spec fromProto(LCM.ChannelSpec channel) {
        return new Channel.Spec(
            channel.getChannelName(),
            ai.lzy.model.grpc.ProtoConverter.fromProto(channel.getScheme())
        );
    }

}
