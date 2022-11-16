package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.grpc.ProtoConverter;
import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerPrivateService extends LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerPrivateService.class);

    private final ChannelStorage channelStorage;
    private final OperationDao operationStorage;
    private final ChannelController channelController;
    private final ExecutorService longrunningExecutor;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerPrivateService(ChannelStorage channelStorage) {
        this.channelStorage = channelStorage;
    }

    @Override
    public void create(LCMPS.ChannelCreateRequest request, StreamObserver<LCMPS.ChannelCreateResponse> response) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Create channel failed, invalid argument");
            response.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        final String channelName = request.getChannelSpec().getChannelName();
        String operationDescription = "Create channel " + channelName;
        LOG.info(operationDescription);

        final String executionId = request.getExecutionId();
        final LCM.ChannelSpec channelSpec = request.getChannelSpec();
        final String channelId = "channel-" + executionId.replaceAll("[^a-zA-z0-9-]+", "-") + "-" + channelName;

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.insertChannel(channelId, executionId, ProtoConverter.fromProto(channelSpec), null));
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
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Destroy channel failed, invalid argument");
            response.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        final String channelId = request.getChannelId();
        String operationDescription = "Destroy channel " + channelId;
        LOG.info(operationDescription);

        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markChannelDestroying(channelId, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = new Operation("ChannelManager", operationDescription,
            Any.pack(LCMPS.ChannelDestroyMetadata.getDefaultInstance()));
        try {
            operationStorage.create(operation, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        response.onNext(operation.toProto());
        response.onCompleted();
        LOG.info(operationDescription + " responded, async operation started");

        longrunningExecutor.submit(() -> {
            try {
                channelController.destroy(channelId);
            } catch (CancellingChannelGraphStateException e) {
                LOG.warn("[executeBind] operation {} cancelled, " + e.getMessage());
                bindOperation.setError(Status.CANCELLED);
                operationStorage.update(bindOperation);
            } catch (Exception e) {
                LOG.error("[executeBind] operation {} failed, " + e.getMessage());
                // error op
            }

            bindOperation.setResponse(Any.pack(LCMS.BindRequest.getDefaultInstance()));
            operationStorage.update(bindOperation);
        });
    }

    @Override
    public void destroyAll(LCMPS.ChannelDestroyAllRequest request, StreamObserver<LongRunning.Operation> response) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Destroying all channels for execution failed, invalid argument");
            response.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        final String executionId = request.getExecutionId();
        String operationDescription = "Destroying all channels for execution " + executionId;
        LOG.info(operationDescription);

        final List<Channel> channelsToDestroy;
        try {
            channelsToDestroy = channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        for (final Channel channel : channelsToDestroy) {
            try (final var guard = lockManager.withLock(channel.id())) {
                withRetries(defaultRetryPolicy(), LOG, () ->
                    channelStorage.markChannelDestroying(channel.id(), null));
            } catch (Exception e) {
                LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
                response.onError(Status.INTERNAL.withCause(e).asException());
                return;
            }
        }

        final Operation operation = new Operation("ChannelManager", operationDescription,
            Any.pack(LCMPS.ChannelDestroyAllMetadata.getDefaultInstance()));
        try {
            operationStorage.create(operation, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        response.onNext(operation.toProto());
        response.onCompleted();
        LOG.info(operationDescription + " responded, async operation started");

        longrunningExecutor.submit(() -> {
            for (final Channel channel : channelsToDestroy) {
                try {
                    channelController.destroy(channel.id());
                } catch (Exception e) {

                }
            }
        });
    }

    @Override
    public void status(LCMPS.ChannelStatusRequest request,
                       StreamObserver<LCMPS.ChannelStatusResponse> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Get status for channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Get status for channel {}", request.getChannelId());

        final String channelId = request.getChannelId();
        final Channel channel;
        try {
            channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
        } catch (Exception e) {
            LOG.error("Get status for channel {} failed, got exception: {}", channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        if (channel == null) {
            LOG.error("Get status for channel {} failed, channel not found", request.getChannelId());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }

        responseObserver.onNext(ProtoConverter.createChannelStatusResponse(channel));
        LOG.info("Get status for channel {} done", channelId);
        responseObserver.onCompleted();
    }

    @Override
    public void statusAll(LCMPS.ChannelStatusAllRequest request,
                          StreamObserver<LCMPS.ChannelStatusAllResponse> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Get status for channels of execution failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Get status for channels of execution {}", request.getExecutionId());

        final String executionId = request.getExecutionId();
        List<Channel> channels;
        try {
            channels = channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
        } catch (Exception e) {
            LOG.error("Get status for channels of execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(ProtoConverter.createChannelStatusAllResponse(channels));
        LOG.info("Get status for channels of execution {} done", executionId);
        responseObserver.onCompleted();
    }
}
