package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.exceptions.CancellingChannelGraphStateException;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static ai.lzy.channelmanager.grpc.ProtoConverter.fromProto;
import static ai.lzy.channelmanager.grpc.ProtoConverter.toProto;
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
    public ChannelManagerPrivateService(ChannelStorage channelStorage, OperationDao operationStorage,
                                        ChannelController channelController, GrainedLock lockManager) {
        this.channelStorage = channelStorage;
        this.operationStorage = operationStorage;
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
            withRetries(LOG, () -> channelStorage.insertChannel(
                channelId, executionId, fromProto(channelSpec), null));
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
            withRetries(LOG, () -> channelStorage.markChannelDestroying(channelId, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation = new Operation("ChannelManager", operationDescription,
            Any.pack(LCMPS.ChannelDestroyMetadata.getDefaultInstance()));
        try {
            withRetries(LOG, () -> operationStorage.create(operation, null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        response.onNext(operation.toProto());
        response.onCompleted();
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());

        longrunningExecutor.submit(() -> {
            try {
                LOG.info(operationDescription + " async operation started, operationId={}", operation.id());

                channelController.destroy(channelId);

                try {
                    withRetries(LOG, () -> operationStorage.updateResponse(operation.id(),
                        Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()).toByteArray(), null));
                } catch (Exception e) {
                    LOG.error("Cannot update operation", e);
                    return;
                }

                LOG.info(operationDescription + " async operation finished, operationId={}", operation.id());
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
            channelsToDestroy = channelStorage.listChannels(executionId, null);
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, got exception: {}", e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        for (final Channel channel : channelsToDestroy) {
            try (final var guard = lockManager.withLock(channel.id())) {
                withRetries(LOG, () -> channelStorage.markChannelDestroying(channel.id(), null));
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
        LOG.info(operationDescription + " responded, async operation scheduled, operationId={}", operation.id());

        longrunningExecutor.submit(() -> {
            try {
                LOG.info(operationDescription + " async operation started, operationId={}", operation.id());

                for (final Channel channel : channelsToDestroy) {
                    channelController.destroy(channel.id());
                }

                try {
                    withRetries(LOG, () -> operationStorage.updateResponse(operation.id(),
                        Any.pack(LCMPS.ChannelDestroyResponse.getDefaultInstance()).toByteArray(), null));
                } catch (Exception e) {
                    LOG.error("Cannot update operation", e);
                    return;
                }

                LOG.info(operationDescription + " async operation finished, operationId={}", operation.id());
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
            channel = channelStorage.findChannel(channelId, Channel.LifeStatus.ALIVE, null);
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

        responseObserver.onNext(createChannelStatusResponse(channel));
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
            channels = channelStorage.listChannels(executionId, null);
        } catch (Exception e) {
            LOG.error("Get status for channels of execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        List<Channel> aliveChannels = channels.stream()
            .filter(ch -> ch.lifeStatus() == Channel.LifeStatus.ALIVE)
            .collect(Collectors.toList());
        responseObserver.onNext(createChannelStatusAllResponse(aliveChannels));
        LOG.info("Get status for channels of execution {} done", executionId);
        responseObserver.onCompleted();
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

    private LCMPS.ChannelStatusResponse createChannelStatusResponse(Channel channel) {
        return LCMPS.ChannelStatusResponse.newBuilder()
            .setStatus(LCMPS.ChannelStatus.newBuilder()
                .setChannel(toChannelProto(channel))
                .build())
            .build();
    }

    private LCMPS.ChannelStatusAllResponse createChannelStatusAllResponse(List<Channel> channels) {
        return LCMPS.ChannelStatusAllResponse.newBuilder()
            .addAllStatuses(channels.stream()
                .map(channel -> LCMPS.ChannelStatus.newBuilder()
                    .setChannel(toChannelProto(channel))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private LCM.Channel toChannelProto(Channel channel) {
        Channel.Senders existedSenders = channel.existedSenders();
        LCM.ChannelSenders.Builder sendersBuilder = LCM.ChannelSenders.newBuilder();
        if (existedSenders.portalEndpoint() != null) {
            sendersBuilder.setPortalSlot(ProtoConverter.toProto(existedSenders.portalEndpoint().slot()));
        }
        if (existedSenders.workerEndpoint() != null) {
            sendersBuilder.setWorkerSlot(ProtoConverter.toProto(existedSenders.workerEndpoint().slot()));
        }

        Channel.Receivers existedReceivers = channel.existedReceivers();
        LCM.ChannelReceivers.Builder receiversBuilder = LCM.ChannelReceivers.newBuilder();
        if (existedReceivers.portalEndpoint() != null) {
            sendersBuilder.setPortalSlot(ProtoConverter.toProto(existedReceivers.portalEndpoint().slot()));
        }
        receiversBuilder.addAllWorkerSlots(existedReceivers.workerEndpoints().stream()
            .map(e -> ProtoConverter.toProto(e.slot()))
            .toList());

        return LCM.Channel.newBuilder()
            .setChannelId(channel.id())
            .setSpec(toProto(channel.spec()))
            .setExecutionId(channel.executionId())
            .setSenders(sendersBuilder.build())
            .setReceivers(receiversBuilder.build())
            .build();
    }
}
