package ai.lzy.channelmanager;

import static ai.lzy.model.GrpcConverter.from;
import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.ChannelImpl;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.channelmanager.control.ChannelController;
import ai.lzy.channelmanager.control.DirectChannelController;
import ai.lzy.channelmanager.control.SnapshotChannelController;
import ai.lzy.channelmanager.graph.LocalChannelGraph;
import ai.lzy.iam.clients.stub.AuthenticateServiceStub;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.channel.DirectChannelSpec;
import ai.lzy.model.channel.SnapshotChannelSpec;
import ai.lzy.model.db.DaoException;
import ai.lzy.model.db.NotFoundException;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.ChannelManager.ChannelCreateRequest;
import ai.lzy.v1.ChannelManager.ChannelCreateResponse;
import ai.lzy.v1.ChannelManager.ChannelDestroyAllRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyAllResponse;
import ai.lzy.v1.ChannelManager.ChannelDestroyRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyResponse;
import ai.lzy.v1.ChannelManager.ChannelStatus;
import ai.lzy.v1.ChannelManager.ChannelStatusList;
import ai.lzy.v1.ChannelManager.ChannelStatusRequest;
import ai.lzy.v1.ChannelManager.ChannelsStatusRequest;
import ai.lzy.v1.ChannelManager.SlotAttach;
import ai.lzy.v1.ChannelManager.SlotAttachStatus;
import ai.lzy.v1.ChannelManager.SlotDetach;
import ai.lzy.v1.ChannelManager.SlotDetachStatus;
import ai.lzy.v1.Channels;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManager {

    private static final Logger LOG = LogManager.getLogger(ChannelManager.class);
    private static final Options options = new Options();

    static {
        options.addRequiredOption("p", "port", true, "gRPC port setting");
        options.addRequiredOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
    }

    private final ChannelStorage channelStorage;
    private final Server channelManagerServer;
    private final URI whiteboardAddress;
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
        final int port = Integer.parseInt(parse.getOptionValue('p', "8122"));
        final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));

        try (ApplicationContext context = ApplicationContext.run(Map.of(
            "channel-manager.port", port,
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
            close();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        channelManagerServer.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void close() {
        channelManagerServer.shutdownNow();
        iamChannel.shutdown();
    }

    public ChannelManager(ApplicationContext ctx) {
        var config = ctx.getBean(ChannelManagerConfig.class);
        channelStorage = new LocalChannelStorage();
        final HostAndPort address = HostAndPort.fromString(config.address());
        final var iamAddress = HostAndPort.fromString(config.iam().address());
        iamChannel = ChannelBuilder.forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
        channelManagerServer = NettyServerBuilder.forAddress(
                new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceStub()))
            .addService(new ChannelManagerService())
            .build();
        whiteboardAddress = URI.create(config.whiteboardAddress());
    }

    private class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

        private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

        private final DataSource dataSource;
        private final ChannelManagerStorage channelManagerStorage;

        @Inject
        private ChannelManagerService(DataSource dataSource, ChannelManagerStorage channelStorage) {
            this.dataSource = dataSource;
            this.channelManagerStorage = channelStorage;
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
                final String channelName = request.getChannelSpec().getChannelName();

                // Channel id is not random uuid for better logs.
                final String channelId = "channel_" + userId + workflowId + channelName;

                final Channels.ChannelSpec channelSpec = request.getChannelSpec();
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
                Transaction.execute(dataSource, conn -> {
                    channelManagerStorage.insertChannel(conn,
                        channelId, userId, workflowId, channelName, channelType, spec
                    );
                    return true;
                });

                responseObserver.onNext(ChannelCreateResponse.newBuilder()
                    .setChannelId(channelId)
                    .build()
                );
                LOG.info("Create channel {} done, channelId={}", channelName, channelId);
                responseObserver.onCompleted();
            } catch (Exception e) {
                LOG.error("Create channel {} failed, got exception: {}",
                    request.getChannelSpec().getChannelName(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }
        }

        @Override
        public void destroy(
            ChannelDestroyRequest request,
            StreamObserver<ChannelDestroyResponse> responseObserver
        ) {
            LOG.info("Destroy channel {}", request.getChannelId());
            try {
                final String channelId = request.getChannelId();

                final AtomicReference<Channel> channel = new AtomicReference<>();
                Transaction.execute(dataSource, conn -> {
                    channel.set(channelManagerStorage.findChannel(conn, true, channelId));
                    if (channel.get() == null) {
                        String errorMessage = String.format("Channel with id %s not found", channelId);
                        LOG.error(errorMessage);
                        throw new NotFoundException(errorMessage);
                    }
                    channelManagerStorage.setChannelLifeStatus(conn, Stream.of(channelId), "Destroying");
                    return true;
                });

                channel.get().destroy();
                Transaction.execute(dataSource, conn -> {
                    channelManagerStorage.removeChannel(conn, channelId);
                    return true;
                });

                responseObserver.onNext(ChannelDestroyResponse.getDefaultInstance());
                LOG.info("Destroy channel {} done", channelId);
                responseObserver.onCompleted();
            } catch (NotFoundException e) {
                LOG.error("Destroy channel {} failed, channel not found",
                    request.getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
            } catch (Exception e) {
                LOG.error("Destroy channel {} failed, got exception: {}",
                    request.getChannelId(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }
        }

        @Override
        public void destroyAll(
            ChannelDestroyAllRequest request,
            StreamObserver<ChannelDestroyAllResponse> responseObserver
        ) {
            LOG.info("Destroying all channels for workflow {}", request.getWorkflowId());
            try {
                final var authenticationContext = AuthenticationContext.current();
                final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
                final String workflowId = request.getWorkflowId();

                List<Channel> channels = new ArrayList<>();
                Transaction.execute(dataSource, conn -> {
                    channels.addAll(channelManagerStorage
                        .listChannels(conn, true, userId, workflowId)
                        .toList());
                    channelManagerStorage.setChannelLifeStatus(conn,
                        channels.stream().map(Channel::id), "Destroying"
                    );
                    return true;
                });

                for (final Channel channel : channels) {
                    channel.destroy();
                    Transaction.execute(dataSource, conn -> {
                        channelManagerStorage.removeChannel(conn, channel.id());
                        return true;
                    });
                }

                responseObserver.onNext(ChannelDestroyAllResponse.getDefaultInstance());
                LOG.info("Destroying all channels for workflow {} done, removed channels: {}",
                    workflowId, channels.stream().map(Channel::id).collect(Collectors.joining(",")));
                responseObserver.onCompleted();
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

                final AtomicReference<Channel> channel = new AtomicReference<>();
                Transaction.execute(dataSource, conn -> {
                    channel.set(channelManagerStorage.findChannel(conn, false, channelId));
                    if (channel.get() == null) {
                        String errorMessage = String.format("Channel with id %s not found", channelId);
                        LOG.error(errorMessage);
                        throw new NotFoundException(errorMessage);
                    }
                    return true;
                });

                responseObserver.onNext(toChannelStatus(channel.get()));
                LOG.info("Get status for channel {} done", request.getChannelId());
                responseObserver.onCompleted();
            } catch (NotFoundException e) {
                LOG.error("Get status for channel {} failed, channel not found",
                    request.getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asException());
            } catch (Exception e) {
                LOG.error("Get status for channel {} failed, got exception: {}",
                    request.getChannelId(), e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }
        }

        @Override
        public void channelsStatus(ChannelsStatusRequest request, StreamObserver<ChannelStatusList> responseObserver) {
            LOG.info("ChannelManager channels status {}", JsonUtils.printRequest(request));
            final ChannelStatusList.Builder builder = ChannelStatusList.newBuilder();
            // TODO not all channels, channels by workflow id
            channelStorage.channels()
                .map(this::toChannelStatus)
                .forEach(builder::addStatuses);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void bind(SlotAttach attach, StreamObserver<SlotAttachStatus> responseObserver) {
            LOG.info("ChannelManager::bind slot={} to channel={}; {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                JsonUtils.printRequest(attach));
            final SlotInstance slotInstance = from(attach.getSlotInstance());
            final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
            final String channelId = slotInstance.channelId();
            final Channel channel = channelStorage.get(channelId);
            if (channel == null) {
                responseObserver.onError(
                    Status.NOT_FOUND
                        .withDescription("Channel with id " + channelId + " is not registered")
                        .asRuntimeException()
                );
                return;
            }

            try {
                final Channel boundChannel = channelStorage.bound(endpoint);
                if (boundChannel != null && !boundChannel.id().equals(channelId)) {
                    responseObserver.onError(
                        Status.INTERNAL
                            .withDescription("Endpoint " + endpoint + " is bound to another channel: " + channel.name())
                            .asRuntimeException()
                    );
                    return;
                }

                channel.bind(endpoint);
                responseObserver.onNext(SlotAttachStatus.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (ChannelException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
            }
        }

        @Override
        public void unbind(SlotDetach detach, StreamObserver<SlotDetachStatus> responseObserver) {
            LOG.info("ChannelManager::unbind slot={} to channel={}; {}",
                detach.getSlotInstance().getSlot(),
                detach.getSlotInstance().getChannelId(),
                JsonUtils.printRequest(detach));
            final SlotInstance slotInstance = from(detach.getSlotInstance());
            final SlotEndpoint endpoint = SlotEndpoint.getInstance(slotInstance);
            final String channelId = slotInstance.channelId();
            final Channel channel = channelStorage.get(channelId);
            if (channel == null) {
                responseObserver.onError(
                    Status.NOT_FOUND
                        .withDescription(
                            "Attempt to unbind endpoint " + endpoint + " from unregistered channel " + channelId)
                        .asRuntimeException()
                );
                return;
            }

            try {
                channel.unbind(endpoint);
                responseObserver.onNext(SlotDetachStatus.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (ChannelException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
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
    }

    private class ChannelManagerStorage {

        private static final Logger LOG = LogManager.getLogger(ChannelManagerStorage.class);

        private final Storage storage;
        private final ObjectMapper objectMapper;

        @Inject
        private ChannelManagerStorage(Storage storage, ObjectMapper objectMapper) {
            this.storage = storage;
            this.objectMapper = objectMapper;
        }

        void insertChannel(
            Connection sqlConnection,
            String channelId,
            String userId,
            String workflowId,
            String channelName,
            Channels.ChannelSpec.TypeCase channelType,
            ChannelSpec channelSpec
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO channels(
                    channel_id,
                    user_id,
                    workflow_id,
                    channel_name,
                    channel_type,
                    channel_spec,
                    created_at,
                    channel_life_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)
            ) {
                String channelSpecJson = objectMapper.writeValueAsString(channelSpec);
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.setString(++index, channelName);
                st.setString(++index, channelType.name());
                st.setString(++index, channelSpecJson);
                st.setTimestamp(++index, Timestamp.from(Instant.now()));
                st.setString(++index, "ALIVE");
                st.executeUpdate();
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to insert channel (channelId={})", channelId, e);
                throw new DaoException(e);
            }
        }

        void removeChannel(Connection sqlConnection, String channelId) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "DELETE FROM channels WHERE channel_id = ?"
            )) {
                int index = 0;
                st.setString(++index, channelId);
                st.execute();
            } catch (SQLException e) {
                LOG.error("Failed to remove channel (channelId={})", channelId, e);
                throw new DaoException(e);
            }
        }

        @Nullable
        Channel findChannel(Connection sqlConnection, boolean forUpdate, String channelId) throws DaoException {
            try {
                final Channel.Builder channelBuilder = ChannelImpl.newBuilder();
                try (final PreparedStatement channelSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        user_id,
                        workflow_id,
                        channel_name,
                        channel_spec,
                        created_at,
                        channel_life_status
                    FROM channels WHERE channel_id = ? AND channel_life_status = ?
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    channelSt.setString(++index, channelId);
                    channelSt.setString(++index, "ALIVE");
                    ResultSet rs = channelSt.executeQuery();
                    if (!rs.next()) {
                        LOG.warn("Channel {} doesn't exist", channelId);
                        return null;
                    }
                    channelBuilder.setId(channelId);
                    var channelType = Channels.ChannelSpec.TypeCase.valueOf(rs.getString("channel_type"));
                    switch (channelType) {
                        case DIRECT -> {
                            channelBuilder.setSpec(objectMapper.readValue(
                                rs.getString("channel_spec"), DirectChannelSpec.class
                            ));
                            channelBuilder.setController(new DirectChannelController());
                        }
                        case SNAPSHOT -> {
                            SnapshotChannelSpec spec = objectMapper.readValue(
                                rs.getString("channel_spec"), SnapshotChannelSpec.class
                            );
                            channelBuilder.setSpec(spec);
                            channelBuilder.setController(new SnapshotChannelController(
                                spec.entryId(), spec.snapshotId(), rs.getString("user_id"), spec.whiteboardAddress()
                            ));
                        }
                        default -> {
                            final String errorMessage = String.format(
                                "Unexpected chanel type \"%s\", only snapshot and direct channels are supported",
                                channelType
                            );
                            LOG.error(errorMessage);
                            throw new NotImplementedException(errorMessage);
                        }
                    }
                }

                try (final PreparedStatement endpointsSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        slot_uri,
                        direction,
                        task_id,
                        slot_spec
                    FROM channel_endpoints WHERE channel_id = ?
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    endpointsSt.setString(++index, channelId);
                    final ResultSet endpointsRs = endpointsSt.executeQuery();
                    while (endpointsRs.next()) {
                        var endpoint = SlotEndpoint.getInstance(new SlotInstance(
                            objectMapper.readValue(endpointsRs.getString("slot_spec"), Slot.class),
                            endpointsRs.getString("task_id"),
                            endpointsRs.getString("channel_id"),
                            URI.create(endpointsRs.getString("slot_uri"))
                        ));
                        switch (endpoint.slotSpec().direction()) {
                            case OUTPUT -> channelBuilder.addSender(endpoint);
                            case INPUT -> channelBuilder.addReceiver(endpoint);
                        }
                    }
                }

                try (final PreparedStatement connectionsSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        sender_uri,
                        receiver_uri,
                    FROM endpoint_connections WHERE channel_id = ?
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    connectionsSt.setString(++index, channelId);
                    final ResultSet connectionsRs = connectionsSt.executeQuery();
                    while (connectionsRs.next()) {
                        channelBuilder.addEdge(
                            connectionsRs.getString("sender_uri"),
                            connectionsRs.getString("receiver_uri")
                        );
                    }
                }

                return channelBuilder.build();
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to find channel (channelId={})", channelId, e);
                throw new DaoException(e);
            }
        }

        Stream<Channel> listChannels(Connection sqlConnection, boolean forUpdate,
            String userId, String workflowId
        ) throws DaoException {
            try {
                Map<String, Channel.Builder> chanelBuildersById = new HashMap<>();
                List<String> channelIds = new ArrayList<>();
                try (final PreparedStatement channelSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        user_id,
                        workflow_id,
                        channel_name,
                        channel_spec,
                        created_at,
                        channel_life_status
                    FROM channels WHERE user_id = ? AND workflow_id = ? AND channel_life_status = ?
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    channelSt.setString(++index, userId);
                    channelSt.setString(++index, workflowId);
                    channelSt.setString(++index, "ALIVE");
                    ResultSet rs = channelSt.executeQuery();
                    while (rs.next()) {
                        final String channelId = rs.getString("channel_id");
                        chanelBuildersById.put(channelId, ChannelImpl.newBuilder().setId(channelId));
                        channelIds.add(channelId);
                        var channelType = Channels.ChannelSpec.TypeCase.valueOf(rs.getString("channel_type"));
                        switch (channelType) {
                            case DIRECT -> {
                                chanelBuildersById.get(channelId).setSpec(objectMapper.readValue(
                                    rs.getString("channel_spec"), DirectChannelSpec.class
                                ));
                                chanelBuildersById.get(channelId).setController(new DirectChannelController());
                            }
                            case SNAPSHOT -> {
                                SnapshotChannelSpec spec = objectMapper.readValue(
                                    rs.getString("channel_spec"), SnapshotChannelSpec.class
                                );
                                chanelBuildersById.get(channelId).setSpec(spec);
                                chanelBuildersById.get(channelId).setController(new SnapshotChannelController(
                                    spec.entryId(), spec.snapshotId(), userId, spec.whiteboardAddress()
                                ));
                            }
                            default -> {
                                final String errorMessage = String.format(
                                    "Unexpected chanel type \"%s\", only snapshot and direct channels are supported",
                                    channelType
                                );
                                LOG.error(errorMessage);
                                throw new NotImplementedException(errorMessage);
                            }
                        }
                    }
                }

                try (final PreparedStatement endpointsSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        slot_uri,
                        direction,
                        task_id,
                        slot_spec
                    FROM channel_endpoints WHERE channel_id = ANY (?)
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    endpointsSt.setArray(++index, sqlConnection.createArrayOf("text", channelIds.toArray()));
                    final ResultSet endpointsRs = endpointsSt.executeQuery();
                    while (endpointsRs.next()) {
                        final String channelId = endpointsRs.getString("channel_id");
                        final var endpoint = SlotEndpoint.getInstance(new SlotInstance(
                            objectMapper.readValue(endpointsRs.getString("slot_spec"), Slot.class),
                            endpointsRs.getString("task_id"),
                            endpointsRs.getString("channel_id"),
                            URI.create(endpointsRs.getString("slot_uri"))
                        ));
                        switch (endpoint.slotSpec().direction()) {
                            case OUTPUT -> chanelBuildersById.get(channelId).addSender(endpoint);
                            case INPUT -> chanelBuildersById.get(channelId).addReceiver(endpoint);
                        }
                    }
                }

                try (final PreparedStatement connectionsSt = sqlConnection.prepareStatement("""
                    SELECT
                        channel_id,
                        sender_uri,
                        receiver_uri,
                    FROM endpoint_connections WHERE channel_id = ANY (?)
                    """ + (forUpdate ? "FOR UPDATE" : ""))
                ) {
                    int index = 0;
                    connectionsSt.setArray(++index, sqlConnection.createArrayOf("text", channelIds.toArray()));
                    final ResultSet connectionsRs = connectionsSt.executeQuery();
                    while (connectionsRs.next()) {
                        final String channelId = connectionsRs.getString("channel_id");
                        chanelBuildersById.get(channelId).addEdge(
                            connectionsRs.getString("sender_uri"),
                            connectionsRs.getString("receiver_uri")
                        );
                    }
                }

                return chanelBuildersById.values().stream().map(Channel.Builder::build);
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to list channels (workflow_id={})", workflowId, e);
                throw new DaoException(e);
            }
        }

        void setChannelLifeStatus(Connection sqlConnection,
            Stream<String> channelIds, String channelLifeStatus
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
               "UPDATE channels SET channel_life_status = ? WHERE channel_id = ANY(?)"
            )) {
                int index = 0;
                st.setArray(++index, sqlConnection.createArrayOf("text", channelIds.toArray()));
                st.setString(++index, channelLifeStatus);
                st.executeUpdate();
            } catch (SQLException e) {
                LOG.error("Failed to update channel life status (channelIds=[{}])",
                    channelIds.collect(Collectors.joining(",")), e);
                throw new DaoException(e);
            }
        }


    }

}
