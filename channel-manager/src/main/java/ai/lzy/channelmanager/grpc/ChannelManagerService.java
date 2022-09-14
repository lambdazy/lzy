package ai.lzy.channelmanager.grpc;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

import ai.lzy.channelmanager.ChannelManagerConfig;
import ai.lzy.channelmanager.channel.*;
import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.db.ChannelStorage;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelManagerDataSource dataSource;
    private final ChannelStorage channelStorage;
    private final URI whiteboardAddress;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerService(ChannelManagerDataSource dataSource, ChannelManagerConfig config,
                                 ChannelStorage channelStorage)
    {
        this.dataSource = dataSource;
        this.channelStorage = channelStorage;
        this.whiteboardAddress = URI.create(config.getWhiteboardAddress());
        this.lockManager = new GrainedLock(config.getLockBucketsCount());
    }

    @Override
    public void create(LCMS.ChannelCreateRequest request, StreamObserver<LCMS.ChannelCreateResponse> responseObserver) {
        LOG.info("Create channel {}", request.getChannelSpec().getChannelName());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String workflowId = request.getWorkflowId();
            final LCM.ChannelSpec channelSpec = request.getChannelSpec();
            if (workflowId.isBlank() || !ProtoValidator.isValid(channelSpec)) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Create channel {} failed, invalid argument: {}",
                    request.getChannelSpec().getChannelName(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            final String channelName = channelSpec.getChannelName();
            // Channel id is not random uuid for better logs.
            /* TODO: change channel id generation after creating Portal
                 final String channelId = String.join("-", "channel", userId, workflowId, channelName)
                    .replaceAll("[^a-zA-z0-9-]+", "-");
             */
            final String channelId = String.join("-", "channel", userId, workflowId).replaceAll("[^a-zA-z0-9-]+", "-")
                                     + "!" + channelName;

            final var channelType = channelSpec.getTypeCase();
            final ChannelSpec spec = switch (channelSpec.getTypeCase()) {
                case DIRECT -> new DirectChannelSpec(
                    channelSpec.getChannelName(),
                    GrpcConverter.contentTypeFrom(channelSpec.getContentType())
                );
                case SNAPSHOT -> new SnapshotChannelSpec(
                    channelSpec.getChannelName(),
                    GrpcConverter.contentTypeFrom(channelSpec.getContentType()),
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
                lockManager.lock(channelId);
                try {
                    channelStorage.insertChannel(channelId, userId, workflowId, channelName, channelType, spec, null);
                } finally {
                    lockManager.unlock(channelId);
                }
            });

            responseObserver.onNext(LCMS.ChannelCreateResponse.newBuilder()
                .setChannelId(channelId)
                .build()
            );
            LOG.info("Create channel {} done, channelId={}", channelName, channelId);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Create channel {} failed, invalid argument: {}",
                request.getChannelSpec().getChannelName(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Create channel {} failed, got exception: {}",
                request.getChannelSpec().getChannelName(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void destroy(LCMS.ChannelDestroyRequest request,
                        StreamObserver<LCMS.ChannelDestroyResponse> responseObserver)
    {
        LOG.info("Destroy channel {}", request.getChannelId());

        try {
            final String channelId = request.getChannelId();
            if (channelId.isBlank()) {
                String errorMessage = "Empty channel id, request shouldn't contain empty fields";
                LOG.error("Destroy channel {} failed, invalid argument: {}", request.getChannelId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            lockManager.lock(channelId);
            try {
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.setChannelLifeStatus(
                    channelId, ChannelStorage.ChannelLifeStatus.DESTROYING, null));

                final Channel channel =
                    channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
                if (channel == null) {
                    String errorMessage = "Channel with id " + channelId + " not found";
                    LOG.error("Destroy channel {} failed, channel not found", request.getChannelId());
                    responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                    return;
                }

                channel.destroy();
                withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channelId, null));

                responseObserver.onNext(LCMS.ChannelDestroyResponse.getDefaultInstance());
                LOG.info("Destroy channel {} done", channelId);
                responseObserver.onCompleted();
            } finally {
                lockManager.unlock(channelId);
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
    public void destroyAll(LCMS.ChannelDestroyAllRequest request,
                           StreamObserver<LCMS.ChannelDestroyAllResponse> responseObserver)
    {
        LOG.info("Destroying all channels for workflow {}", request.getWorkflowId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String workflowId = request.getWorkflowId();
            if (workflowId.isBlank()) {
                String errorMessage = "Empty workflow id, request shouldn't contain empty fields";
                LOG.error("Destroying all channels for workflow {} failed, invalid argument: {}",
                    request.getWorkflowId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.setChannelLifeStatus(
                userId, workflowId, ChannelStorage.ChannelLifeStatus.DESTROYING, null));

            List<Channel> channels =
                channelStorage.listChannels(userId, workflowId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
            for (final Channel channel : channels) {
                lockManager.lock(channel.id());
                try {
                    channel.destroy();
                    withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeChannel(channel.id(), null));
                } finally {
                    lockManager.unlock(channel.id());
                }
            }

            responseObserver.onNext(LCMS.ChannelDestroyAllResponse.getDefaultInstance());
            LOG.info("Destroying all channels for workflow {} done, {} removed channels: {}",
                workflowId, channels.size(),
                channels.stream().map(Channel::id).collect(Collectors.joining(",")));
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Destroying all channels for workflow {} failed, invalid argument: {}",
                request.getWorkflowId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Destroying all channels for workflow {} failed, got exception: {}",
                request.getWorkflowId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void status(LCMS.ChannelStatusRequest request,
                       StreamObserver<LCMS.ChannelStatus> responseObserver)
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

            responseObserver.onNext(ProtoConverter.toChannelStatusProto(channel));
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
    public void statusAll(LCMS.ChannelStatusAllRequest request,
                          StreamObserver<LCMS.ChannelStatusList> responseObserver)
    {
        LOG.info("Get status for channels of workflow {}", request.getWorkflowId());

        try {
            final var authenticationContext = AuthenticationContext.current();
            final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
            final String workflowId = request.getWorkflowId();
            if (workflowId.isBlank()) {
                String errorMessage = "Empty workflow id, request shouldn't contain empty fields";
                LOG.error("Destroying all channels for workflow {} failed, invalid argument: {}",
                    request.getWorkflowId(), errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            List<Channel> channels =
                channelStorage.listChannels(userId, workflowId, ChannelStorage.ChannelLifeStatus.ALIVE, null);

            final LCMS.ChannelStatusList.Builder builder = LCMS.ChannelStatusList.newBuilder();
            channels.forEach(channel -> builder.addStatuses(ProtoConverter.toChannelStatusProto(channel)));
            var channelStatusList = builder.build();

            responseObserver.onNext(channelStatusList);
            LOG.info("Get status for channels of workflow {} done", request.getWorkflowId());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Destroying all channels for workflow {} failed, invalid argument: {}",
                request.getWorkflowId(), e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Get status for channels of workflow {} failed, got exception: {}",
                request.getWorkflowId(), e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void bind(LCMS.SlotAttach attach,
                     StreamObserver<LCMS.SlotAttachStatus> responseObserver)
    {
        LOG.info("Bind slot={} to channel={}",
            attach.getSlotInstance().getSlot().getName(),
            attach.getSlotInstance().getChannelId());

        if (!ProtoValidator.isValid(attach.getSlotInstance())) {
            String errorMessage = "Request shouldn't contain empty fields";
            LOG.error("Bind slot={} to channel={} failed, invalid argument: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            return;
        }

        lockManager.lock(attach.getSlotInstance().getChannelId());
        try {
            final SlotInstance slotInstance = GrpcConverter.from(attach.getSlotInstance());
            final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
            final String channelId = endpoint.slotInstance().channelId();

            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel with id " + channelId + " not found";
                LOG.error("Bind slot={} to channel={} failed, channel not found",
                    attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
                responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            final Stream<Endpoint> newBound = channel.bind(endpoint);

            withRetries(defaultRetryPolicy(), LOG, () -> {
                try (final var transaction = TransactionHandle.create(dataSource)) {

                    channelStorage.insertEndpoint(endpoint, transaction);

                    final Map<Endpoint, Endpoint> addedEdges = switch (endpoint.slotSpec().direction()) {
                        case OUTPUT -> newBound.collect(Collectors.toMap(e -> endpoint, e -> e));
                        case INPUT -> newBound.collect(Collectors.toMap(e -> e, e -> endpoint));
                    };
                    channelStorage.insertEndpointConnections(channelId, addedEdges, transaction);

                    transaction.commit();
                }
            });

            responseObserver.onNext(LCMS.SlotAttachStatus.getDefaultInstance());
            LOG.info("Bind slot={} to channel={} done",
                attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
            responseObserver.onCompleted();
        } catch (ChannelException | IllegalArgumentException e) {
            LOG.error("Bind slot={} to channel={} failed, invalid argument: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Bind slot={} to channel={} failed, got exception: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        } finally {
            lockManager.unlock(attach.getSlotInstance().getChannelId());
        }
    }

    @Override
    public void unbind(LCMS.SlotDetach detach,
                       StreamObserver<LCMS.SlotDetachStatus> responseObserver)
    {
        LOG.info("Unbind slot={} to channel={}",
            detach.getSlotInstance().getSlot(),
            detach.getSlotInstance().getChannelId());

        if (!ProtoValidator.isValid(detach.getSlotInstance())) {
            String errorMessage = "Request shouldn't contain empty fields";
            LOG.error("Unbind slot={} to channel={} failed, invalid argument: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            return;
        }

        lockManager.lock(detach.getSlotInstance().getChannelId());
        try {
            final SlotInstance slotInstance = GrpcConverter.from(detach.getSlotInstance());
            final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
            final String channelId = endpoint.slotInstance().channelId();

            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel with id " + channelId + " not found";
                LOG.error("Unbind slot={} to channel={} failed, channel not found",
                    detach.getSlotInstance().getSlot().getName(), detach.getSlotInstance().getChannelId());
                responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            channel.unbind(endpoint);

            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.removeEndpointWithConnections(endpoint, null));

            responseObserver.onNext(LCMS.SlotDetachStatus.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (ChannelException | IllegalArgumentException e) {
            LOG.error("Unbind slot={} to channel={} failed, invalid argument: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        } catch (Exception e) {
            LOG.error("Unbind slot={} to channel={} failed, got exception: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        } finally {
            lockManager.unlock(detach.getSlotInstance().getChannelId());
        }
    }

}
