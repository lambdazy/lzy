package ai.lzy.channelmanager;

import static ai.lzy.model.GrpcConverter.from;
import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.channel.DirectChannelSpec;
import ai.lzy.model.channel.SnapshotChannelSpec;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.ChannelManager.ChannelCreateRequest;
import ai.lzy.v1.ChannelManager.ChannelCreateResponse;
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
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public ChannelManager(ApplicationContext ctx) {
        var config = ctx.getBean(ChannelManagerConfig.class);
        channelStorage = new LocalChannelStorage();
        channelManagerServer = NettyServerBuilder.forPort(config.port())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new ChannelManagerService())
            .build();
        whiteboardAddress = URI.create(config.whiteboardAddress());
    }

    private ChannelStatus channelStatus(Channel channel) {
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

    private class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
        @Override
        public void create(ChannelCreateRequest request,
                           StreamObserver<ChannelCreateResponse> responseObserver) {
            LOG.info("ChannelManager create channel {}: {}",
                request.getChannelSpec().getChannelName(),
                JsonUtils.printRequest(request));

            final Channels.ChannelSpec channelSpec = request.getChannelSpec();
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
                default -> throw new NotImplementedException("Only snapshot and direct channels are supported");
            };
            final Channel channel = channelStorage.create(spec, request.getWorkflowId());
            responseObserver.onNext(
                ChannelCreateResponse.newBuilder()
                    .setChannelId(channel.id())
                    .build()
            );
            responseObserver.onCompleted();
        }

        @Override
        public void destroy(ChannelDestroyRequest request, StreamObserver<ChannelDestroyResponse> responseObserver) {
            LOG.info("ChannelManager destroy channel {}", request.getChannelId());
            try {
                channelStorage.destroy(request.getChannelId());
                responseObserver.onNext(ChannelDestroyResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (ChannelException e) {
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asRuntimeException());
            }
        }

        @Override
        public void status(ChannelStatusRequest request, StreamObserver<ChannelStatus> responseObserver) {
            LOG.info("ChannelManager channel status {}", request.getChannelId());
            final Channel channel = channelStorage.get(request.getChannelId());
            if (channel == null) {
                responseObserver.onError(Status.NOT_FOUND.withDescription("Channel not found").asRuntimeException());
                return;
            }

            responseObserver.onNext(channelStatus(channel));
            responseObserver.onCompleted();
        }

        @Override
        public void channelsStatus(ChannelsStatusRequest request, StreamObserver<ChannelStatusList> responseObserver) {
            LOG.info("ChannelManager channels status {}", JsonUtils.printRequest(request));
            final ChannelStatusList.Builder builder = ChannelStatusList.newBuilder();
            channelStorage.channels()
                .map(ChannelManager.this::channelStatus)
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

        //    @Override
        //    public void unbindAll(UUID sessionId) {
        //        LOG.info("LocalChannelsRepository::unbindAll sessionId=" + sessionId);
        //        for (Channel channel : channels.values()) {
        //            final Lock lock = lockManager.getOrCreate(channel.name());
        //            lock.lock();
        //            try {
        //                //unbind receivers
        //                channel
        //                    .bound()
        //                    .filter(endpoint -> endpoint.taskId().equals(sessionId))
        //                    .filter(endpoint -> endpoint.slot().direction() == Direction.INPUT)
        //                    .forEach(endpoint -> {
        //                        try {
        //                            channel.unbind(endpoint);
        //                        } catch (ChannelException e) {
        //                            LOG.warn("Fail to unbind " + endpoint + " from channel " + channel);
        //                        }
        //                    });
        //                //unbind senders
        //                channel
        //                    .bound()
        //                    .filter(endpoint -> endpoint.taskId().equals(sessionId))
        //                    .filter(endpoint -> endpoint.slot().direction() == Direction.OUTPUT)
        //                    .forEach(endpoint -> {
        //                        try {
        //                            channel.unbind(endpoint);
        //                        } catch (ChannelException e) {
        //                            LOG.warn("Fail to unbind " + endpoint + " from channel " + channel);
        //                        }
        //                    });
        //            } finally {
        //                lock.unlock();
        //            }
        //        }
        //    }
    }

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
            channelManagerServer.shutdown();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        channelManagerServer.awaitTermination();
    }

    public void close() {
        channelManagerServer.shutdownNow();
    }
}
