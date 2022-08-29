package ai.lzy.channelmanager;

import static ai.lzy.channelmanager.db.ChannelStorage.ChannelLifeStatus;
import static ai.lzy.model.GrpcConverter.from;
import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.channelmanager.db.ChannelStorage;
import ai.lzy.iam.clients.stub.AuthenticateServiceStub;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.channel.DirectChannelSpec;
import ai.lzy.model.channel.SnapshotChannelSpec;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.ChannelManager.*;
import ai.lzy.v1.Channels;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.Operations;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManager {

    private static final Logger LOG = LogManager.getLogger(ChannelManager.class);
    private static final Options options = new Options();

    static {
        options.addRequiredOption("p", "port", true, "gRPC port setting");
        options.addRequiredOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
    }

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("channel-manager", options);
            System.exit(-1);
        }
        final URI address = URI.create(parse.getOptionValue('a', "localhost:8122"));
        final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));

        try (ApplicationContext context = ApplicationContext.run(Map.of(
            "channel-manager.address", address,
            "channel-manager.whiteboard-address", whiteboardAddress.toString()
        ))) {
            final ChannelManager channelManager = new ChannelManager(context);
            channelManager.start();
            channelManager.awaitTermination();
        }
    }

    public void start() throws IOException {
        channelManagerServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("gRPC server is shutting down!");
            stop();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        channelManagerServer.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void stop() {
        channelManagerServer.shutdownNow();
        iamChannel.shutdown();
    }

    public ChannelManager(ApplicationContext ctx) {
        var config = ctx.getBean(ChannelManagerConfig.class);
        final HostAndPort address = HostAndPort.fromString(config.getAddress());
        final var iamAddress = HostAndPort.fromString(config.getIam().getAddress());
        iamChannel = ChannelBuilder.forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
        channelManagerServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceStub()))
            .addService(ctx.getBean(ChannelManagerService.class))
            .build();
    }

    @Singleton
    private static class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

        private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

        private final ChannelManagerDataSource dataSource;
        private final ChannelStorage channelStorage;
        private final URI whiteboardAddress;
        private final LockManager lockManager;

        @Inject
        public ChannelManagerService(
            ChannelManagerDataSource dataSource, ChannelManagerConfig config, ChannelStorage channelStorage
        ) {
            this.dataSource = dataSource;
            this.channelStorage = channelStorage;
            this.whiteboardAddress = URI.create(config.getWhiteboardAddress());
            this.lockManager = new LocalLockManager().withPrefix("channel-manager");
        }

        @Override
        public void create(
            ChannelCreateRequest request,
            StreamObserver<ChannelCreateResponse> responseObserver
        ) {
            LOG.info("Create channel {}: {}",
                request.getChannelSpec().getChannelName(),
                JsonUtils.printRequest(request));

            try {
                final var authenticationContext = AuthenticationContext.current();
                final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
                final String workflowId = request.getWorkflowId();
                final Channels.ChannelSpec channelSpec = request.getChannelSpec();
                if (workflowId.isBlank() || !isChannelSpecValid(channelSpec)) {
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
                final String channelId = String.join("-", "channel", userId, workflowId)
                    .replaceAll("[^a-zA-z0-9-]+", "-") + "!" + channelName;
                lockManager.getOrCreate(channelId);
                try {
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

                    channelStorage.insertChannel(channelId, userId, workflowId, channelName, channelType, spec, null);
                } finally {
                    lockManager.remove(channelId);
                }

                responseObserver.onNext(ChannelCreateResponse.newBuilder()
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
        public void destroy(ChannelDestroyRequest request, StreamObserver<ChannelDestroyResponse> responseObserver) {
            LOG.info("Destroy channel {}", request.getChannelId());

            try {
                final String channelId = request.getChannelId();
                if (channelId.isBlank()) {
                    String errorMessage = "Empty channel id, request shouldn't contain empty fields";
                    LOG.error("Destroy channel {} failed, invalid argument: {}", request.getChannelId(), errorMessage);
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                    return;
                }
                lockManager.getOrCreate(channelId);

                channelStorage.setChannelLifeStatus(channelId, ChannelLifeStatus.DESTROYING, null);

                final Channel channel = channelStorage.findChannel(channelId, ChannelLifeStatus.DESTROYING, null);
                if (channel == null) {
                    String errorMessage = "Channel with id " + channelId + " not found";
                    LOG.error("Destroy channel {} failed, channel not found", request.getChannelId());
                    responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                    return;
                }

                channel.destroy();
                channelStorage.removeChannel(channelId, null);

                responseObserver.onNext(ChannelDestroyResponse.getDefaultInstance());
                LOG.info("Destroy channel {} done", channelId);
                responseObserver.onCompleted();
            } catch (IllegalArgumentException e) {
                LOG.error("Destroy channel {} failed, invalid argument: {}",
                    request.getChannelId(), e.getMessage(), e);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            } catch (Exception e) {
                LOG.error("Destroy channel {} failed, got exception: {}",
                    request.getChannelId(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            } finally {
                lockManager.remove(request.getChannelId());
            }
        }

        @Override
        public void destroyAll(ChannelDestroyAllRequest request,
                               StreamObserver<ChannelDestroyAllResponse> responseObserver)
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

                channelStorage.setChannelLifeStatus(userId, workflowId, ChannelLifeStatus.DESTROYING, null);

                List<Channel> channels = channelStorage.listChannels(
                    userId, workflowId, ChannelLifeStatus.DESTROYING, null
                );
                for (final Channel channel : channels) {
                    try {
                        lockManager.getOrCreate(channel.id());
                        channel.destroy();
                        channelStorage.removeChannel(channel.id(), null);
                    } finally {
                        lockManager.remove(channel.id());
                    }
                }

                responseObserver.onNext(ChannelDestroyAllResponse.getDefaultInstance());
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
        public void status(ChannelStatusRequest request, StreamObserver<ChannelStatus> responseObserver) {
            LOG.info("Get status for channel {}", request.getChannelId());

            try {
                final String channelId = request.getChannelId();
                if (channelId.isBlank()) {
                    String errorMessage = "Empty channel id, request shouldn't contain empty fields";
                    LOG.error("Destroy channel {} failed, invalid argument: {}", request.getChannelId(), errorMessage);
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                    return;
                }

                final Channel channel = channelStorage.findChannel(channelId, ChannelLifeStatus.ALIVE, null);
                if (channel == null) {
                    String errorMessage = "Channel with id " + channelId + " not found";
                    LOG.error("Get status for channel {} failed, channel not found", request.getChannelId());
                    responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                    return;
                }

                responseObserver.onNext(toChannelStatus(channel));
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
        public void statusAll(ChannelStatusAllRequest request, StreamObserver<ChannelStatusList> responseObserver) {
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

                List<Channel> channels = channelStorage.listChannels(userId, workflowId, ChannelLifeStatus.ALIVE, null);

                final ChannelStatusList.Builder builder = ChannelStatusList.newBuilder();
                channels.forEach(channel -> builder.addStatuses(toChannelStatus(channel)));
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
        public void bind(SlotAttach attach, StreamObserver<SlotAttachStatus> responseObserver) {
            LOG.info("Bind slot={} to channel={}; {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                JsonUtils.printRequest(attach));

            if (!isSlotInstanceValid(attach.getSlotInstance())) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Bind slot={} to channel={} failed, invalid argument: {}",
                    attach.getSlotInstance().getSlot().getName(),
                    attach.getSlotInstance().getChannelId(),
                    errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            lockManager.getOrCreate(attach.getSlotInstance().getChannelId());
            try {
                final SlotInstance slotInstance = from(attach.getSlotInstance());
                final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
                final String channelId = endpoint.slotInstance().channelId();

                final Channel channel;
                try (final var transaction = new TransactionHandle(dataSource)) {
                    channel = channelStorage.findChannel(channelId, ChannelLifeStatus.ALIVE, transaction);
                    if (channel == null) {
                        String errorMessage = "Channel with id " + channelId + " not found";
                        LOG.error("Bind slot={} to channel={} failed, channel not found",
                            attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
                        responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                        return;
                    }

                    channelStorage.insertEndpoint(endpoint, transaction);

                    transaction.commit();
                }

                final Stream<Endpoint> newBound;
                try {
                    newBound = channel.bind(endpoint);
                } catch (Exception e) {
                    channelStorage.removeEndpointWithConnections(endpoint, null);
                    throw e;
                }

                final Map<Endpoint, Endpoint> addedEdges = switch (endpoint.slotSpec().direction()) {
                    case OUTPUT -> newBound.collect(Collectors.toMap(e -> endpoint, e -> e));
                    case INPUT -> newBound.collect(Collectors.toMap(e -> e, e -> endpoint));
                };
                channelStorage.insertEndpointConnections(channelId, addedEdges, null);

                responseObserver.onNext(SlotAttachStatus.getDefaultInstance());
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
                lockManager.remove(attach.getSlotInstance().getChannelId());
            }
        }

        @Override
        public void unbind(SlotDetach detach, StreamObserver<SlotDetachStatus> responseObserver) {
            LOG.info("Unbind slot={} to channel={}; {}",
                detach.getSlotInstance().getSlot(),
                detach.getSlotInstance().getChannelId(),
                JsonUtils.printRequest(detach));

            if (!isSlotInstanceValid(detach.getSlotInstance())) {
                String errorMessage = "Request shouldn't contain empty fields";
                LOG.error("Unbind slot={} to channel={} failed, invalid argument: {}",
                    detach.getSlotInstance().getSlot().getName(),
                    detach.getSlotInstance().getChannelId(),
                    errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }

            lockManager.getOrCreate(detach.getSlotInstance().getChannelId());
            try {
                final SlotInstance slotInstance = from(detach.getSlotInstance());
                final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
                final String channelId = endpoint.slotInstance().channelId();

                final Channel channel;
                try (final var transaction = new TransactionHandle(dataSource)) {
                    channel = channelStorage.findChannel(channelId, ChannelLifeStatus.ALIVE, transaction);
                    if (channel == null) {
                        String errorMessage = "Channel with id " + channelId + " not found";
                        LOG.error("Unbind slot={} to channel={} failed, channel not found",
                            detach.getSlotInstance().getSlot().getName(), detach.getSlotInstance().getChannelId());
                        responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                        return;
                    }

                    channel.unbind(endpoint);
                    channelStorage.removeEndpointWithConnections(endpoint, transaction);

                    transaction.commit();
                }

                responseObserver.onNext(SlotDetachStatus.getDefaultInstance());
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
                lockManager.remove(detach.getSlotInstance().getChannelId());
            }
        }

        private ChannelStatus toChannelStatus(Channel channel) {
            final ChannelStatus.Builder statusBuilder = ChannelStatus.newBuilder();
            statusBuilder
                .setChannelId(channel.id())
                .setChannelSpec(to(channel.spec()));
            channel.slotsStatus()
                .map(slotStatus ->
                    Operations.SlotStatus.newBuilder()
                        .setTaskId(slotStatus.tid())
                        .setConnectedTo(channel.id())
                        .setDeclaration(to(slotStatus.slot()))
                        .setPointer(slotStatus.pointer())
                        .setState(Operations.SlotStatus.State.valueOf(slotStatus.state().toString()))
                        .build())
                .forEach(statusBuilder::addConnected);
            return statusBuilder.build();
        }

        private boolean isChannelSpecValid(Channels.ChannelSpec channelSpec) {
            try {
                boolean isValid = true;
                isValid = isValid && !channelSpec.getChannelName().isBlank();
                isValid = isValid && channelSpec.getTypeCase().getNumber() != 0;
                isValid = isValid && !channelSpec.getContentType().getType().isBlank();
                isValid = isValid && channelSpec.getContentType().getSchemeType().getNumber() != 0;
                return isValid;
            } catch (NullPointerException e) {
                return false;
            }
        }

        private boolean isSlotInstanceValid(LzyFsApi.SlotInstance slotInstance) {
            try {
                boolean isValid = true;
                isValid = isValid && !slotInstance.getTaskId().isBlank();
                isValid = isValid && !slotInstance.getSlotUri().isBlank();
                isValid = isValid && !slotInstance.getChannelId().isBlank();
                isValid = isValid && !slotInstance.getSlot().getName().isBlank();
                isValid = isValid && !slotInstance.getSlot().getContentType().getType().isBlank();
                isValid = isValid && slotInstance.getSlot().getContentType().getSchemeType().getNumber() != 0;
                return isValid;
            } catch (NullPointerException e) {
                return false;
            }
        }
    }

}
