package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.channel.DirectChannelSpec;
import ai.lzy.channelmanager.channel.SnapshotChannelSpec;
import ai.lzy.channelmanager.db.ChannelStorage;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerPrivateService extends LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerPrivateService.class);

    private final ChannelStorage channelStorage;
    private final URI whiteboardAddress;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerPrivateService(ChannelManagerConfig config, ChannelStorage channelStorage,
                                        GrainedLock lockManager)
    {
        this.channelStorage = channelStorage;
        this.whiteboardAddress = URI.create(config.getWhiteboardAddress());
        this.lockManager = lockManager;
    }

    @Override
    public void create(LCMPS.ChannelCreateRequest request, StreamObserver<LCMPS.ChannelCreateResponse> response) {
        LOG.info("Create channel {}", request.getChannelSpec().getChannelName());

        try {
            final String executionId = request.getExecutionId();
            final LCM.ChannelSpec channelSpec = request.getChannelSpec();
            if (executionId.isBlank() || !isValid(channelSpec)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Create channel {} failed, invalid argument: {}",
                    request.getChannelSpec().getChannelName(), errorMessage);
                response.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            final String channelName = channelSpec.getChannelName();
            // Channel id is not random uuid for better logs.
            /* TODO: change channel id generation after creating Portal
                 final String channelId = String.join("-", "channel", executionId, channelName)
                    .replaceAll("[^a-zA-z0-9-]+", "-");
             */
            final String channelId = String.join("-", "channel", executionId).replaceAll("[^a-zA-z0-9-]+", "-")
                                     + "!" + channelName;

            final var channelType = channelSpec.getTypeCase();
            final ChannelSpec spec = switch (channelSpec.getTypeCase()) {
                case DIRECT -> new DirectChannelSpec(
                    channelSpec.getChannelName(),
                    ProtoConverter.fromProto(channelSpec.getContentType())
                );
                case SNAPSHOT -> new SnapshotChannelSpec(
                    channelSpec.getSnapshot().getUserId(),
                    channelSpec.getChannelName(),
                    ProtoConverter.fromProto(channelSpec.getContentType()),
                    channelSpec.getSnapshot().getSnapshotId(),
                    channelSpec.getSnapshot().getEntryId(),
                    whiteboardAddress
                );
                default -> {
                    final String errorMessage = String.format(
                        "Unexpected chanel type \"%s\", only snapshot and direct channels are supported",
                        channelSpec.getTypeCase()
                    );
                    LOG.error(errorMessage);
                    throw new NotImplementedException(errorMessage);
                }
            };

            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (var guard = lockManager.withLock(channelId)) {
                    channelStorage.insertChannel(channelId, executionId, channelName, channelType, spec, null);
                }
            });

            response.onNext(LCMPS.ChannelCreateResponse.newBuilder()
                .setChannelId(channelId)
                .build()
            );
            LOG.info("Create channel {} done, channelId={}", channelName, channelId);
            response.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Create channel {} failed, invalid argument: {}",
                request.getChannelSpec().getChannelName(), e.getMessage(), e);
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Create channel {} failed, got exception: {}",
                request.getChannelSpec().getChannelName(), e.getMessage(), e);
            response.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void destroy(LCMPS.ChannelDestroyRequest request,
                        StreamObserver<LCMPS.ChannelDestroyResponse> responseObserver)
    {
        LOG.info("Destroy channel {}", request.getChannelId());

        final String channelId = request.getChannelId();
        if (channelId.isBlank()) {
            String errorMessage = "Empty channel id, request shouldn't contain empty fields";
            LOG.error("Destroy channel {} failed, invalid argument: {}", request.getChannelId(), errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            return;
        }

        try {
            Channel channel;
            try (var guard = lockManager.withLock(channelId)) {
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.setChannelLifeStatus(
                    channelId, ChannelStorage.ChannelLifeStatus.DESTROYING, null));

                channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
                if (channel != null) {
                    channel.destroy();
                    withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channelId, null));
                }
            }

            if (channel != null) {
                responseObserver.onNext(LCMPS.ChannelDestroyResponse.getDefaultInstance());
                LOG.info("Destroy channel {} done", channelId);
                responseObserver.onCompleted();
            } else {
                String errorMessage = "Channel with id " + channelId + " not found";
                LOG.error("Destroy channel {} failed, channel not found", request.getChannelId());
                responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
            }
        } catch (IllegalArgumentException e) {
            LOG.error("Destroy channel {} failed, invalid argument: {}",
                request.getChannelId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Destroy channel {} failed, got exception: {}",
                request.getChannelId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void destroyAll(LCMPS.ChannelDestroyAllRequest request,
                           StreamObserver<LCMPS.ChannelDestroyAllResponse> responseObserver)
    {
        LOG.info("Destroying all channels for execution {}", request.getExecutionId());

        try {
            final String executionId = request.getExecutionId();
            if (executionId.isBlank()) {
                String errorMessage = "Empty execution id, request shouldn't contain empty fields";
                LOG.error("Destroying all channels for execution {} failed, invalid argument: {}",
                    request.getExecutionId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.setChannelLifeStatusByExecution(
                executionId, ChannelStorage.ChannelLifeStatus.DESTROYING, null));

            List<Channel> channels =
                channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
            for (final Channel channel : channels) {
                try (var guard = lockManager.withLock(channel.id())) {
                    channel.destroy();
                    withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channel.id(), null));
                }
            }

            responseObserver.onNext(LCMPS.ChannelDestroyAllResponse.getDefaultInstance());
            LOG.info("Destroying all channels for execution {} done, {} removed channels: {}",
                executionId, channels.size(),
                channels.stream().map(Channel::id).collect(Collectors.joining(",")));
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Destroying all channels for execution {} failed, invalid argument: {}",
                request.getExecutionId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Destroying all channels for execution {} failed, got exception: {}",
                request.getExecutionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void status(LCMPS.ChannelStatusRequest request,
                       StreamObserver<LCMPS.ChannelStatus> responseObserver)
    {
        LOG.info("Get status for channel {}", request.getChannelId());

        try {
            final String channelId = request.getChannelId();
            if (channelId.isBlank()) {
                String errorMessage = "Empty channel id, request shouldn't contain empty fields";
                LOG.error("Destroy channel {} failed, invalid argument: {}", request.getChannelId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel with id " + channelId + " not found";
                LOG.error("Get status for channel {} failed, channel not found", request.getChannelId());
                responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            responseObserver.onNext(toChannelStatusProto(channel));
            LOG.info("Get status for channel {} done", request.getChannelId());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Destroy channel {} failed, invalid argument: {}",
                request.getChannelId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Get status for channel {} failed, got exception: {}",
                request.getChannelId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void statusAll(LCMPS.ChannelStatusAllRequest request,
                          StreamObserver<LCMPS.ChannelStatusList> responseObserver)
    {
        LOG.info("Get status for channels of execution {}", request.getExecutionId());

        try {
            final String executionId = request.getExecutionId();
            if (executionId.isBlank()) {
                String errorMessage = "Empty execution id, request shouldn't contain empty fields";
                LOG.error("Destroying all channels for execution {} failed, invalid argument: {}",
                    request.getExecutionId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            List<Channel> channels =
                channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            final LCMPS.ChannelStatusList.Builder builder = LCMPS.ChannelStatusList.newBuilder();
            channels.forEach(channel -> builder.addStatuses(toChannelStatusProto(channel)));
            var channelStatusList = builder.build();

            responseObserver.onNext(channelStatusList);
            LOG.info("Get status for channels of execution {} done", request.getExecutionId());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Destroying all channels for execution {} failed, invalid argument: {}",
                request.getExecutionId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Get status for channels of execution {} failed, got exception: {}",
                request.getExecutionId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    private LCMPS.ChannelStatus toChannelStatusProto(Channel channel) {
        final LCMPS.ChannelStatus.Builder statusBuilder = LCMPS.ChannelStatus.newBuilder();
        statusBuilder
            .setChannelId(channel.id())
            .setChannelSpec(LCM.ChannelSpec.newBuilder()
                .setChannelName(channel.name())
                .setContentType(ai.lzy.model.grpc.ProtoConverter.toProto(channel.spec().contentType()))
                .build());
        channel.slotsStatus()
            .map(slotStatus ->
                LMS.SlotStatus.newBuilder()
                    .setTaskId(slotStatus.tid())
                    .setConnectedTo(channel.id())
                    .setDeclaration(ai.lzy.model.grpc.ProtoConverter.toProto(slotStatus.slot()))
                    .setPointer(slotStatus.pointer())
                    .setState(LMS.SlotStatus.State.valueOf(slotStatus.state().toString()))
                    .build())
            .forEach(statusBuilder::addConnected);
        return statusBuilder.build();
    }

    private boolean isValid(ai.lzy.v1.channel.LCM.ChannelSpec channelSpec) {
        try {
            boolean isValid = true;
            isValid = isValid && !channelSpec.getChannelName().isBlank();
            isValid = isValid && channelSpec.getTypeCase().getNumber() != 0;
            isValid = isValid && !channelSpec.getContentType().getDataFormat().isBlank();
            isValid = isValid && !channelSpec.getContentType().getSchemeFormat().isBlank();
            return isValid;
        } catch (NullPointerException e) {
            return false;
        }
    }

}
