package ai.lzy.channelmanager;

import static ai.lzy.model.GrpcConverter.from;
import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.ChannelImpl;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.channelmanager.control.DirectChannelController;
import ai.lzy.channelmanager.control.SnapshotChannelController;
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
import ai.lzy.model.db.Transaction;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.ChannelManager.ChannelCreateRequest;
import ai.lzy.v1.ChannelManager.ChannelCreateResponse;
import ai.lzy.v1.ChannelManager.ChannelDestroyAllRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyAllResponse;
import ai.lzy.v1.ChannelManager.ChannelDestroyRequest;
import ai.lzy.v1.ChannelManager.ChannelDestroyResponse;
import ai.lzy.v1.ChannelManager.ChannelStatus;
import ai.lzy.v1.ChannelManager.ChannelStatusAllRequest;
import ai.lzy.v1.ChannelManager.ChannelStatusList;
import ai.lzy.v1.ChannelManager.ChannelStatusRequest;
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
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManager {

    private static final Logger LOG = LogManager.getLogger(ChannelManager.class);
    private static final Options options = new Options();

    static {
        options.addRequiredOption("p", "port", true, "gRPC port setting");
        options.addRequiredOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
    }

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
        final HostAndPort address = HostAndPort.fromString(config.address());
        final var iamAddress = HostAndPort.fromString(config.iam().address());
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
        whiteboardAddress = URI.create(config.whiteboardAddress());
    }

    private class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

        private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

        private final ChannelManagerDataSource dataSource;
        private final ChannelStorage channelStorage;

        @Inject
        private ChannelManagerService(ChannelManagerDataSource dataSource, ChannelStorage channelStorage) {
            this.dataSource = dataSource;
            this.channelStorage = channelStorage;
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
                    channelStorage.insertChannel(conn,
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
                    channel.set(channelStorage.findChannel(conn, true, channelId));
                    if (channel.get() == null) {
                        String errorMessage = String.format("Channel with id %s not found", channelId);
                        LOG.error(errorMessage);
                        throw new NotFoundException(errorMessage);
                    }
                    channelStorage.setChannelLifeStatus(conn, Stream.of(channelId), "Destroying");
                    return true;
                });

                channel.get().destroy();
                Transaction.execute(dataSource, conn -> {
                    channelStorage.removeChannel(conn, channelId);
                    return true;
                });

                responseObserver.onNext(ChannelDestroyResponse.getDefaultInstance());
                LOG.info("Destroy channel {} done", channelId);
                responseObserver.onCompleted();
            } catch (NotFoundException e) {
                LOG.error("Destroy channel {} failed, channel not found", request.getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
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
                    channels.addAll(channelStorage.listChannels(conn, true, userId, workflowId).toList());
                    channelStorage.setChannelLifeStatus(conn,
                        channels.stream().map(Channel::id), "Destroying"
                    );
                    return true;
                });

                for (final Channel channel : channels) {
                    channel.destroy();
                    Transaction.execute(dataSource, conn -> {
                        channelStorage.removeChannel(conn, channel.id());
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
                    channel.set(channelStorage.findChannel(conn, false, channelId));
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
                LOG.error("Get status for channel {} failed, channel not found", request.getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
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

                List<Channel> channels = new ArrayList<>();
                Transaction.execute(dataSource, conn -> {
                    channels.addAll(channelStorage.listChannels(conn, false, userId, workflowId).toList());
                    return true;
                });

                final ChannelStatusList.Builder builder = ChannelStatusList.newBuilder();
                channels.forEach(channel -> builder.addStatuses(toChannelStatus(channel)));
                var channelStatusList = builder.build();

                responseObserver.onNext(channelStatusList);
                LOG.info("Get status for channels of workflow {} done", request.getWorkflowId());
                responseObserver.onCompleted();
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

            try {
                final var authenticationContext = AuthenticationContext.current();
                final String userId = Objects.requireNonNull(authenticationContext).getSubject().id();
                final SlotInstance slotInstance = from(attach.getSlotInstance());
                final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
                final String channelId = slotInstance.channelId();

                Transaction.execute(dataSource, conn -> {
                    final Channel channel = channelStorage.findChannel(conn, true, channelId);
                    if (channel == null) {
                        String errorMessage = String.format("Channel with id %s not found", channelId);
                        LOG.error(errorMessage);
                        throw new NotFoundException(errorMessage);
                    }

                    List<String> boundChannels = channelStorage.listBoundChannels(conn, false,
                            userId, channel.ownerWorkflowId(), endpoint.uri().toString());

                    if (boundChannels.size() > 0) {
                        final String errorMessage;
                        if (boundChannels.size() == 1) {
                            final String otherChannelId = boundChannels.get(0);
                            if (channelId.equals(otherChannelId)) {
                                LOG.warn("Endpoint {} is already bound to this channel", endpoint);
                                return false;
                            } else {
                                errorMessage = endpoint + " is bound to another channel " + otherChannelId;
                                LOG.error(errorMessage);
                                responseObserver.onError(
                                    Status.INVALID_ARGUMENT.withDescription(errorMessage).asException()
                                );
                            }
                        } else {
                            errorMessage = endpoint + " is bound to more than one channel";
                            LOG.error(errorMessage);
                            responseObserver.onError(
                                Status.INTERNAL.withDescription(errorMessage).asException()
                            );
                        }
                        return false;
                    }

                    try {
                        channel.bind(endpoint);
                    } catch (ChannelException e) {
                        responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
                        return false;
                    }

                    channelStorage.insertEndpoint(conn, channelId, endpoint);
                    final Map<Endpoint, Endpoint> addedEdges = switch (endpoint.slotSpec().direction()) {
                        case OUTPUT -> channel.bound(endpoint).collect(Collectors.toMap(e -> endpoint, e -> e));
                        case INPUT -> channel.bound(endpoint).collect(Collectors.toMap(e -> e, e -> endpoint));
                    };
                    channelStorage.insertEndpointConnections(conn, channelId, addedEdges);
                    return true;
                });

                responseObserver.onNext(SlotAttachStatus.getDefaultInstance());
                LOG.info("Bind slot={} to channel={} done",
                    attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
                responseObserver.onCompleted();
            } catch (NotFoundException e) {
                LOG.error("Bind slot={} to channel={} failed, channel not found",
                    attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            } catch (Exception e) {
                LOG.error("Bind slot={} to channel={} failed, got exception: {}",
                    attach.getSlotInstance().getSlot().getName(),
                    attach.getSlotInstance().getChannelId(),
                    e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            }
        }

        @Override
        public void unbind(SlotDetach detach, StreamObserver<SlotDetachStatus> responseObserver) {
            LOG.info("Unbind slot={} to channel={}; {}",
                detach.getSlotInstance().getSlot(),
                detach.getSlotInstance().getChannelId(),
                JsonUtils.printRequest(detach));

            try {
                final SlotInstance slotInstance = from(detach.getSlotInstance());
                final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
                final String channelId = slotInstance.channelId();

                Transaction.execute(dataSource, conn -> {
                    final Channel channel = channelStorage.findChannel(conn, true, channelId);
                    if (channel == null) {
                        String errorMessage = String.format("Channel with id %s not found", channelId);
                        LOG.error(errorMessage);
                        throw new NotFoundException(errorMessage);
                    }

                    try {
                        channel.unbind(endpoint);
                    } catch (ChannelException e) {
                        responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
                        return false;
                    }

                    channelStorage.removeEndpointWithConnections(conn, channelId, endpoint);
                    return true;
                });
                responseObserver.onNext(SlotDetachStatus.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (NotFoundException e) {
                LOG.error("Unbind slot={} to channel={} failed, channel not found",
                    detach.getSlotInstance().getSlot().getName(), detach.getSlotInstance().getChannelId(), e);
                responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            } catch (Exception e) {
                LOG.error("Unbind slot={} to channel={} failed, got exception: {}",
                    detach.getSlotInstance().getSlot().getName(),
                    detach.getSlotInstance().getChannelId(),
                    e.getMessage(), e);
                responseObserver.onError(Status.INTERNAL.withCause(e).asException());
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

    private class ChannelStorage {

        private static final Logger LOG = LogManager.getLogger(ChannelStorage.class);

        private final ObjectMapper objectMapper;

        @Inject
        private ChannelStorage(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        void insertChannel(Connection sqlConnection, String channelId, String userId, String workflowId,
            String channelName, Channels.ChannelSpec.TypeCase channelType, ChannelSpec channelSpec
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
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    ch.channel_id as channel_id,
                    ch.user_id as user_id,
                    ch.workflow_id as workflow_id,
                    ch.channel_name as channel_name,
                    ch.channel_spec as channel_spec,
                    ch.created_at as created_at,
                    ch.channel_life_status as channel_life_status,
                    e.slot_uri as slot_uri,
                    e.direction as direction,
                    e.task_id as task_id,
                    e.slot_spec as slot_spec,
                    c.slot_uri as connected_slot_uri
                FROM channels ch
                FULL JOIN channel_endpoints e ON ch.channel_id = e.channel_id
                FULL JOIN endpoint_connections c ON e.channel_id = c.channel_id AND e.slot_uri = c.sender_uri
                WHERE ch.channel_id = ? AND ch.channel_life_status = ?
                """ + (forUpdate ? "FOR UPDATE" : ""))
            ) {
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, "ALIVE");
                Stream<Channel> channels = parseChannels(st.executeQuery());
                return channels.findFirst().orElse(null);
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to find channel (channelId={})", channelId, e);
                throw new DaoException(e);
            }
        }

        Stream<Channel> listChannels(
            Connection sqlConnection, boolean forUpdate, String userId, String workflowId
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    ch.channel_id as channel_id,
                    ch.user_id as user_id,
                    ch.workflow_id as workflow_id,
                    ch.channel_name as channel_name,
                    ch.channel_spec as channel_spec,
                    ch.created_at as created_at,
                    ch.channel_life_status as channel_life_status,
                    e.slot_uri as slot_uri,
                    e.direction as direction,
                    e.task_id as task_id,
                    e.slot_spec as slot_spec,
                    c.slot_uri as connected_slot_uri
                FROM channels ch
                FULL JOIN channel_endpoints e ON ch.channel_id = e.channel_id
                FULL JOIN endpoint_connections c ON e.channel_id = c.channel_id AND e.slot_uri = c.sender_uri
                WHERE ch.user_id = ? AND ch.workflow_id = ? AND ch.channel_life_status = ?
                """ + (forUpdate ? "FOR UPDATE" : ""))
            ) {
                int index = 0;
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.setString(++index, "ALIVE");
                return parseChannels(st.executeQuery());
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to list channels (workflow_id={})", workflowId, e);
                throw new DaoException(e);
            }
        }

        List<String> listBoundChannels(
            Connection sqlConnection, boolean forUpdate, String userId, String workflowId, String slotUri
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT ch.channel_id as channel_id
                FROM channels ch INNER JOIN channel_endpoints e ON ch.channel_id = e.channel_id
                WHERE ch.user_id = ? AND ch.workflow_id = ? AND e.slot_uri = ? AND ch.channel_life_status = ?
                """ + (forUpdate ? "FOR UPDATE" : ""))
            ) {
                int index = 0;
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.setString(++index, slotUri);
                st.setString(++index, "ALIVE");
                ResultSet rs = st.executeQuery();
                List<String> channels = new ArrayList<>();
                while (rs.next()) {
                    channels.add(rs.getString("channel_id"));
                }
                return channels;
            } catch (SQLException e) {
                LOG.error("Failed to list bound channels (slot_uri={})", slotUri, e);
                throw new DaoException(e);
            }
        }

        void insertEndpoint(Connection sqlConnection, String channelId, Endpoint endpoint) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO channel_endpoints(
                    channel_id,
                    slot_uri,
                    direction,
                    task_id,
                    slot_spec
                ) VALUES (?, ?, ?, ?, ?)
                """)
            ) {
                String slotSpecJson = objectMapper.writeValueAsString(endpoint.slotSpec());
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, endpoint.uri().toString());
                st.setString(++index, endpoint.slotSpec().direction().name());
                st.setString(++index, endpoint.taskId());
                st.setString(++index, slotSpecJson);
                st.executeUpdate();
            } catch (SQLException | JsonProcessingException e) {
                LOG.error("Failed to insert channel endpoint ({})", endpoint, e);
                throw new DaoException(e);
            }
        }

        void removeEndpointWithConnections(
            Connection sqlConnection, String channelId, Endpoint endpoint
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "DELETE FROM channel_endpoints WHERE channel_id = ? AND slot_uri = ?"
            )) {
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, endpoint.uri().toString());
                st.execute();
            } catch (SQLException e) {
                LOG.error("Failed to remove channel endpoint ({})", endpoint, e);
                throw new DaoException(e);
            }
        }

        void insertEndpointConnections(
            Connection sqlConnection, String channelId, Map<Endpoint, Endpoint> edges
        ) throws DaoException {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO endpoint_connections (
                    channel_id,
                    sender_uri,
                    receiver_uri
                ) VALUES (?, unnest(?), unnest(?))
                """)
            ) {
                final List<String> senderUris = new ArrayList<>();
                final List<String> receiverUris = new ArrayList<>();
                edges.forEach((sender, receiver) -> {
                    senderUris.add(sender.uri().toString());
                    receiverUris.add(receiver.uri().toString());
                });

                int index = 0;
                st.setString(++index, channelId);
                st.setArray(++index, sqlConnection.createArrayOf("text", senderUris.toArray()));
                st.setArray(++index, sqlConnection.createArrayOf("text", receiverUris.toArray()));
                st.executeUpdate();
            } catch (SQLException e) {
                LOG.error("Failed to insert channel endpoint connections (channel={})", channelId, e);
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

        private Stream<Channel> parseChannels(ResultSet rs) throws SQLException, JsonProcessingException {
            Map<String, Channel.Builder> chanelBuildersById = new HashMap<>();
            Map<String, HashSet<String>> slotsUriByChannelId = new HashMap<>();
            while (rs.next()) {
                final String channelId = rs.getString("channel_id");
                if (!chanelBuildersById.containsKey(channelId)) {
                    chanelBuildersById.put(channelId, ChannelImpl.newBuilder()
                        .setId(channelId)
                        .setOwnerWorkflowId(rs.getString("workflow_id")));
                    slotsUriByChannelId.put(channelId, new HashSet<>());
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
                final String slotUri = rs.getString("slot_uri");
                final String connectedSlotUri = rs.getString("connected_slot_uri");
                if (slotUri != null && !slotsUriByChannelId.get(channelId).contains(slotUri)) {
                    slotsUriByChannelId.get(channelId).add(slotUri);
                    var endpoint = SlotEndpoint.getInstance(new SlotInstance(
                        objectMapper.readValue(rs.getString("slot_spec"), Slot.class),
                        rs.getString("task_id"),
                        channelId,
                        URI.create(slotUri)
                    ));
                    switch (endpoint.slotSpec().direction()) {
                        case OUTPUT -> chanelBuildersById.get(channelId).addSender(endpoint);
                        case INPUT -> chanelBuildersById.get(channelId).addReceiver(endpoint);
                    }
                }
                if (connectedSlotUri != null) {
                    chanelBuildersById.get(channelId).addEdge(slotUri, connectedSlotUri);
                }
            }
            return chanelBuildersById.values().stream().map(Channel.Builder::build);
        }

    }

}
