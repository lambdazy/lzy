package ai.lzy.kharon;

import static ai.lzy.model.UriScheme.LzyFs;
import static ai.lzy.model.UriScheme.LzyKharon;
import static ai.lzy.model.UriScheme.SlotAzure;
import static ai.lzy.model.UriScheme.SlotS3;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.kharon.workflow.WorkflowService;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.SlotConnectionManager;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ProxyClientHeaderInterceptor;
import ai.lzy.model.grpc.ProxyServerHeaderInterceptor;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Kharon;
import ai.lzy.v1.Kharon.ReceivedDataStatus;
import ai.lzy.v1.Kharon.SendSlotDataMessage;
import ai.lzy.v1.Kharon.TerminalCommand;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.Lzy.GetSessionsRequest;
import ai.lzy.v1.Lzy.GetSessionsResponse;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsApi.SlotCommandStatus;
import ai.lzy.v1.LzyFsGrpc;
import ai.lzy.v1.LzyKharonGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.Operations;
import ai.lzy.v1.SnapshotApiGrpc;
import ai.lzy.v1.Tasks;
import ai.lzy.v1.WbApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("UnstableApiUsage")
public class LzyKharon {

    private static final Logger LOG = LogManager.getLogger(LzyKharon.class);
    private static final Options options = new Options();

    static {
        options.addOption("h", "host", true, "Kharon host name");
        options.addOption("e", "external", true, "Kharon external host");
        options.addOption("p", "port", true, "gRPC port setting");
        options.addOption("s", "servant-proxy-port", true, "gRPC servant port setting");
        options.addOption("fs", "servantfs-proxy-port", true, "gRPC servant fs port setting");
        options.addOption("z", "lzy-server-address", true, "Lzy server address [host:port]");
        options.addOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
        options.addOption("lsa", "lzy-snapshot-address", true, "Lzy snapshot address [host:port]");
        options.addOption("ch", "lzy-channel-manager-address", true, "Lzy channel manager address [host:port]");
        options.addOption("iam", "lzy-iam-address", true, "Lzy iam address [host:port]");
    }

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final WbApiGrpc.WbApiBlockingStub whiteboard;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager;
    private final TerminalSessionManager sessionManager;
    private final DataCarrier dataCarrier = new DataCarrier();
    private final SlotConnectionManager connectionManager = new SlotConnectionManager();
    private final Server kharonServer;
    private final Server kharonServantFsProxy;
    private final Server channelManagerProxy;
    private final ManagedChannel serverChannel;
    private final ManagedChannel whiteboardChannel;
    private final ManagedChannel snapshotChannel;
    private final UriResolver uriResolver;

    public LzyKharon(ApplicationContext ctx) throws URISyntaxException {
        var config = ctx.getBean(KharonConfig.class);

        var selfAddress = HostAndPort.fromString(config.address());
        var serverAddress = HostAndPort.fromString(config.serverAddress());
        final URI externalAddress = new URI(LzyKharon.scheme(), null, config.externalHost(), selfAddress.getPort(),
            null, null, null);
        serverChannel = ChannelBuilder.forAddress(serverAddress.getHost(), serverAddress.getPort())
            .usePlaintext()
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        var whiteboardAddress = HostAndPort.fromString(config.whiteboardAddress());
        whiteboardChannel = ChannelBuilder.forAddress(whiteboardAddress.getHost(), whiteboardAddress.getPort())
            .usePlaintext()
            .build();
        whiteboard = WbApiGrpc.newBlockingStub(whiteboardChannel);

        var snapshotAddress = HostAndPort.fromString(config.snapshotAddress());
        snapshotChannel = ChannelBuilder.forAddress(snapshotAddress.getHost(), snapshotAddress.getPort())
            .usePlaintext()
            .build();
        snapshot = SnapshotApiGrpc.newBlockingStub(snapshotChannel);

        var servantProxyFsAddress = new URI(LzyFs.scheme(), null, selfAddress.getHost(), config.servantFsProxyPort(),
            null, null, null);

        sessionManager = new TerminalSessionManager();
        uriResolver = new UriResolver(externalAddress, servantProxyFsAddress);

        var kharonServerBuilder = NettyServerBuilder.forPort(selfAddress.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIMEOUT_SECS,  TimeUnit.SECONDS)
            .addService(ServerInterceptors.intercept(new KharonService()))
            .addService(ServerInterceptors.intercept(new SnapshotService()))
            .addService(ServerInterceptors.intercept(new WhiteboardService()))
            .addService(ServerInterceptors.intercept(new ServerService()));

        if (config.workflow().enabled()) {
            var authInterceptor = new AuthServerInterceptor(
                new AuthenticateServiceGrpcClient(GrpcConfig.from(config.iam().address())));

            kharonServerBuilder.addService(ServerInterceptors.intercept(
                ctx.getBean(WorkflowService.class),
                authInterceptor));
        }

        kharonServer = kharonServerBuilder.build();

        kharonServantFsProxy = NettyServerBuilder.forPort(config.servantFsProxyPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(new KharonServantFsProxyService()))
            .build();

        var channelManagerAddress = HostAndPort.fromString(config.channelManagerAddress());
        ManagedChannel channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddress.getHost(), channelManagerAddress.getPort())
            .usePlaintext()
            .build();
        channelManager = LzyChannelManagerGrpc
            .newBlockingStub(channelManagerChannel)
            .withInterceptors(new ProxyClientHeaderInterceptor());

        channelManagerProxy = NettyServerBuilder.forPort(config.channelManagerProxyPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(
                new ChannelManagerProxyService(),
                new ProxyServerHeaderInterceptor()))
            .build();
    }

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        var p = new HashMap<String, Object>();

        if (args.length > 0) {
            final CommandLineParser cliParser = new DefaultParser();
            final HelpFormatter cliHelp = new HelpFormatter();
            CommandLine parse = null;
            try {
                parse = cliParser.parse(options, args);
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                cliHelp.printHelp("lzy-kharon", options);
                System.exit(-1);
            }

            final String host = parse.getOptionValue('h', "localhost");
            final String externalHost = parse.getOptionValue('e', "api.lzy.ai");
            final int port = Integer.parseInt(parse.getOptionValue('p', "8899"));
            final int servantPort = Integer.parseInt(parse.getOptionValue('s', "8900"));
            final int servantFsPort = parse.hasOption("fs") ? Integer.parseInt(parse.getOptionValue("fs"))
                    : servantPort + 1;
            final int channelManagerProxyPort = Integer.parseInt(
                parse.getOptionValue("channel-manager-proxy-port", "8123"));
            final URI serverAddress = URI.create(parse.getOptionValue('z', "http://localhost:8888"));
            final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));
            final URI snapshotAddress = URI.create(parse.getOptionValue("lzy-snapshot-address", "http://localhost:8999"));
            final URI channelManagerAddress =
                URI.create(parse.getOptionValue("lzy-channel-manager-address", "http://localhost:8122"));
            final URI iamAddress = URI.create(parse.getOptionValue("lzy-iam-address", "http://localhost:8443"));

            p.put("kharon.address", host + ":" + port);
            p.put("kharon.external-host", externalHost);
            p.put("kharon.server-address", serverAddress.getHost() + ":" + serverAddress.getPort());
            p.put("kharon.whiteboard-address", whiteboardAddress.getHost() + ":" + whiteboardAddress.getPort());
            p.put("kharon.snapshot-address", snapshotAddress.getHost() + ":" + snapshotAddress.getPort());
            p.put("kharon.channel-manager-address",
                channelManagerAddress.getHost() + ":" + channelManagerAddress.getPort());
            p.put("kharon.servant-proxy-port", servantPort);
            p.put("kharon.servant-proxy-fs-port", servantFsPort);
            p.put("kharon.channel-manager-proxy-port", channelManagerProxyPort);

            p.put("kharon.iam.address", iamAddress.getHost() + ":" + iamAddress.getPort());
            p.put("kharon.workflow.enabled", "false");
        }

        try (ApplicationContext context = ApplicationContext.run(PropertySource.of("cli", p))) {
            final LzyKharon kharon = new LzyKharon(context);

            kharon.start();
            kharon.awaitTermination();
        }
    }

    public void start() throws IOException {
        kharonServer.start();
        kharonServantFsProxy.start();
        channelManagerProxy.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            kharonServer.shutdown();
            channelManagerProxy.shutdown();
            kharonServantFsProxy.shutdown();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        kharonServer.awaitTermination();
        channelManagerProxy.awaitTermination();
        kharonServantFsProxy.awaitTermination();
    }

    public void close() {
        serverChannel.shutdownNow();
        whiteboardChannel.shutdownNow();
        snapshotChannel.shutdownNow();
        kharonServer.shutdownNow();
        channelManagerProxy.shutdownNow();
        kharonServantFsProxy.shutdownNow();
    }

    private static class ProxyCall {

        public static <ReqT, RespT> void exec(Function<ReqT, RespT> impl, ReqT request,
                                              StreamObserver<RespT> responseObserver) {
            try {
                final RespT response = impl.apply(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Throwable th) {
                responseObserver.onError(th);
            }
        }
    }

    private class SnapshotService extends SnapshotApiGrpc.SnapshotApiImplBase {
        @Override
        public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request,
                                   StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
            ProxyCall.exec(snapshot::createSnapshot, request, responseObserver);
        }

        @Override
        public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request,
                                     StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::finalizeSnapshot, request, responseObserver);
        }

        @Override
        public void lastSnapshot(LzyWhiteboard.LastSnapshotCommand request,
                                 StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
            ProxyCall.exec(snapshot::lastSnapshot, request, responseObserver);
        }

        @Override
        public void entryStatus(LzyWhiteboard.EntryStatusCommand request,
                                StreamObserver<LzyWhiteboard.EntryStatusResponse> responseObserver) {
            ProxyCall.exec(snapshot::entryStatus, request, responseObserver);
        }

        @Override
        public void commit(LzyWhiteboard.CommitCommand request,
                           StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::commit, request, responseObserver);
        }

        @Override
        public void abort(LzyWhiteboard.AbortCommand request,
                          StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::abort, request, responseObserver);
        }

        @Override
        public void prepareToSave(LzyWhiteboard.PrepareCommand request,
                                  StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::prepareToSave, request, responseObserver);
        }

        @Override
        public void createEntry(LzyWhiteboard.CreateEntryCommand request,
                                StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::createEntry, request, responseObserver);
        }

        @Override
        public void saveExecution(LzyWhiteboard.SaveExecutionCommand request,
                                  StreamObserver<LzyWhiteboard.SaveExecutionResponse> responseObserver) {
            ProxyCall.exec(snapshot::saveExecution, request, responseObserver);
        }

        @Override
        public void resolveExecution(LzyWhiteboard.ResolveExecutionCommand request,
                                     StreamObserver<LzyWhiteboard.ResolveExecutionResponse> responseObserver) {
            ProxyCall.exec(snapshot::resolveExecution, request, responseObserver);
        }
    }

    private class WhiteboardService extends WbApiGrpc.WbApiImplBase {
        @Override
        public void createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand request,
                                     StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
            ProxyCall.exec(whiteboard::createWhiteboard, request, responseObserver);
        }

        @Override
        public void whiteboardsList(LzyWhiteboard.WhiteboardsListCommand request,
                                    StreamObserver<LzyWhiteboard.WhiteboardsResponse> responseObserver) {
            ProxyCall.exec(whiteboard::whiteboardsList, request, responseObserver);
        }

        @Override
        public void link(LzyWhiteboard.LinkCommand request,
                         StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(whiteboard::link, request, responseObserver);
        }

        @Override
        public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request,
                                  StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
            ProxyCall.exec(whiteboard::getWhiteboard, request, responseObserver);
        }
    }

    private class KharonService extends LzyKharonGrpc.LzyKharonImplBase {

        @Override
        public StreamObserver<Kharon.TerminalProgress> attachTerminal(
            StreamObserver<TerminalCommand> responseObserver
        ) {
            LOG.info("Kharon::attachTerminal");
            final String sessionId = "terminal_" + UUID.randomUUID();
            final TerminalSession session = sessionManager.createSession(
                sessionId,
                new TerminalController(responseObserver),
                server
            );
            responseObserver.onNext(
                Kharon.TerminalCommand.newBuilder()
                    .setCommandId("terminal_attach_response")
                    .setTerminalAttachResponse(Kharon.TerminalAttachResponse.newBuilder()
                        .setWorkflowId(sessionId)
                        .build())
                    .build());
            Context.current().addListener(context -> sessionManager.deleteSession(sessionId), Runnable::run);
            return session.terminalProgressHandler();
        }

        @Override
        public StreamObserver<SendSlotDataMessage> writeToInputSlot(StreamObserver<ReceivedDataStatus> response) {
            LOG.info("Kharon::writeToInputSlot");
            return dataCarrier.connectTerminalConnection(response);
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI slotUri = UriResolver.parseSlotUri(URI.create(request.getSlotInstance().getSlotUri()));
            if (slotUri == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad uri").asException());
                return;
            }

            LzyFsApi.SlotRequest newRequest = LzyFsApi.SlotRequest.newBuilder()
                .mergeFrom(request)
                .setSlotInstance(
                    LzyFsApi.SlotInstance.newBuilder()
                        .mergeFrom(request.getSlotInstance())
                        .setSlotUri(slotUri.toString())
                        .build())
                .build();

            URI servantFsUri = null;
            try {
                servantFsUri = new URI(LzyFs.scheme(), null, slotUri.getHost(), slotUri.getPort(), null, null, null);
                final LzyFsGrpc.LzyFsBlockingStub servantFs = connectionManager.getOrCreate(servantFsUri);
                LOG.info("Created connection to servant fs " + servantFsUri);
                final Iterator<LzyFsApi.Message> messageIterator = servantFs.openOutputSlot(newRequest);
                while (messageIterator.hasNext()) {
                    responseObserver.onNext(messageIterator.next());
                }
                responseObserver.onCompleted();
                LOG.info("openOutputSlot completed for " + servantFsUri);
            } catch (Exception e) {
                LOG.error("Exception while openOutputSlot " + e);
                responseObserver.onError(e);
            } finally {
                if (servantFsUri != null) {
                    connectionManager.shutdownConnection(servantFsUri);
                }
            }
        }
    }

    private class ServerService extends LzyServerGrpc.LzyServerImplBase {

        @Override
        public void publish(Lzy.PublishRequest request,
                            StreamObserver<Lzy.PublishResponse> responseObserver) {
            ProxyCall.exec(server::publish, request, responseObserver);
        }

        @Override
        public void zygotes(IAM.Auth request,
                            StreamObserver<Operations.ZygoteList> responseObserver) {
            ProxyCall.exec(server::zygotes, request, responseObserver);
        }

        @Override
        public void task(Tasks.TaskCommand request,
                         StreamObserver<Tasks.TaskStatus> responseObserver) {
            ProxyCall.exec(server::task, request, responseObserver);
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> responseObserver) {
            if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                LOG.debug("Kharon::start " + JsonUtils.printRequest(request));
            } else {
                LOG.info("Kharon::start request (tid={})", request.getTid());
            }
            try {
                final Iterator<Tasks.TaskProgress> start = server.start(request);
                while (start.hasNext()) {
                    responseObserver.onNext(start.next());
                }
                LOG.info("Kharon::start user task completed " + request.getAuth().getUser().getUserId());
                responseObserver.onCompleted();
            } catch (Throwable th) {
                responseObserver.onError(th);
            }
        }

        @Override
        public void tasksStatus(IAM.Auth request,
                                StreamObserver<Tasks.TasksList> responseObserver) {
            ProxyCall.exec(server::tasksStatus, request, responseObserver);
        }

        @Override
        public void getS3Credentials(Lzy.GetS3CredentialsRequest request,
                                     StreamObserver<Lzy.GetS3CredentialsResponse> responseObserver) {
            ProxyCall.exec(server::getS3Credentials, request, responseObserver);
        }

        @Override
        public void getBucket(Lzy.GetBucketRequest request, StreamObserver<Lzy.GetBucketResponse> responseObserver) {
            ProxyCall.exec(server::getBucket, request, responseObserver);
        }

        @Override
        public void getSessions(GetSessionsRequest request,
                                StreamObserver<GetSessionsResponse> responseObserver) {
            ProxyCall.exec(server::getSessions, request, responseObserver);
        }
    }

    private class ChannelManagerProxyService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
        @Override
        public void create(ChannelManager.ChannelCreateRequest request,
                           StreamObserver<ChannelManager.ChannelCreateResponse> responseObserver) {
            ProxyCall.exec(channelManager::create, request, responseObserver);
        }

        @Override
        public void destroy(ChannelManager.ChannelDestroyRequest request,
                            StreamObserver<ChannelManager.ChannelDestroyResponse> responseObserver) {
            ProxyCall.exec(channelManager::destroy, request, responseObserver);
        }

        @Override
        public void status(ChannelManager.ChannelStatusRequest request,
                           StreamObserver<ChannelManager.ChannelStatus> responseObserver) {
            ProxyCall.exec(channelManager::status, request, responseObserver);
        }

        @Override
        public void channelsStatus(ChannelManager.ChannelsStatusRequest request,
                                   StreamObserver<ChannelManager.ChannelStatusList> responseObserver) {
            ProxyCall.exec(channelManager::channelsStatus, request, responseObserver);
        }

        @Override
        public void bind(ChannelManager.SlotAttach request,
                         StreamObserver<ChannelManager.SlotAttachStatus> responseObserver) {
            try {
                final ChannelManager.SlotAttach updatedRequest = ChannelManager.SlotAttach.newBuilder()
                    .setSlotInstance(LzyFsApi.SlotInstance.newBuilder(request.getSlotInstance())
                        .setSlotUri(
                            uriResolver.convertToServantFsProxyUri(
                                URI.create(request.getSlotInstance().getSlotUri())
                            ).toString())
                        .build())
                    .build();
                ProxyCall.exec(channelManager::bind, updatedRequest, responseObserver);
            } catch (URISyntaxException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
            }
        }

        @Override
        public void unbind(ChannelManager.SlotDetach request,
                           StreamObserver<ChannelManager.SlotDetachStatus> responseObserver) {
            try {
                final ChannelManager.SlotDetach updatedRequest =
                    ChannelManager.SlotDetach.newBuilder()
                        .setSlotInstance(
                            LzyFsApi.SlotInstance.newBuilder(request.getSlotInstance())
                                .setSlotUri(
                                    uriResolver.convertToServantFsProxyUri(
                                        URI.create(request.getSlotInstance().getSlotUri())
                                    ).toString()
                                ).build()
                        )
                    .build();
                ProxyCall.exec(channelManager::unbind, updatedRequest, responseObserver);
            } catch (URISyntaxException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
            }
        }
    }

    private class KharonServantFsProxyService extends LzyFsGrpc.LzyFsImplBase {

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request,
                                   StreamObserver<LzyFsApi.Message> responseObserver) {
            try {
                final LzyFsApi.SlotInstance slotInstance = request.getSlotInstance();
                final URI slotUri = URI.create(slotInstance.getSlotUri());
                final TerminalSession session = sessionManager.get(slotInstance.getTaskId());
                LOG.info("KharonServantFsProxyService sessionId = " + session.sessionId()
                    + "::openOutputSlot " + JsonUtils.printRequest(request));
                dataCarrier.openServantConnection(slotUri, responseObserver);

                session.terminalController().connectSlot(LzyFsApi.ConnectSlotRequest.newBuilder()
                    .setFrom(slotInstance)
                    .setTo(slotInstance)
                    .build());
            } catch (InvalidSessionRequestException | TerminalController.TerminalControllerResetException e) {
                LOG.warn("Exception while openOutputSlot ", e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asRuntimeException());
            }
        }

        @Override
        public void createSlot(LzyFsApi.CreateSlotRequest request, StreamObserver<SlotCommandStatus> response) {
            call(request, request.getTaskId(), response, (req, controller) -> controller.createSlot(req));
        }

        @Override
        public void connectSlot(LzyFsApi.ConnectSlotRequest request, StreamObserver<SlotCommandStatus> response) {
            try {
                final URI connectToUri = URI.create(request.getTo().getSlotUri());
                if (!SlotS3.match(connectToUri) && !SlotAzure.match(connectToUri)) {
                    final URI convertedToKharonUri = uriResolver.convertToKharonWithSlotUri(connectToUri);
                    request = LzyFsApi.ConnectSlotRequest.newBuilder()
                        .mergeFrom(request)
                        .setTo(LzyFsApi.SlotInstance.newBuilder()
                            .mergeFrom(request.getTo())
                            .setSlotUri(convertedToKharonUri.toString())
                            .build())
                        .build();
                }

                call(request, request.getFrom().getTaskId(), response,
                    (req, controller) -> controller.connectSlot(req));
            } catch (URISyntaxException e) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("Invalid servant uri")
                        .asRuntimeException());
            }
        }

        @Override
        public void disconnectSlot(LzyFsApi.DisconnectSlotRequest request, StreamObserver<SlotCommandStatus> response) {
            call(request, request.getSlotInstance().getTaskId(), response,
                (req, controller) -> controller.disconnectSlot(req));
        }

        @Override
        public void statusSlot(LzyFsApi.StatusSlotRequest request, StreamObserver<SlotCommandStatus> response) {
            call(request, request.getSlotInstance().getTaskId(), response,
                (req, controller) -> controller.statusSlot(req));
        }

        @Override
        public void destroySlot(LzyFsApi.DestroySlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
            call(request, request.getSlotInstance().getTaskId(), resp,
                (req, controller) -> controller.destroySlot(req));
        }

        interface SlotFn<R> {
            SlotCommandStatus call(R request, TerminalController controller)
                    throws TerminalController.TerminalControllerResetException;
        }

        private <R> void call(
            R request,
            String sessionId,
            StreamObserver<SlotCommandStatus> responseObserver,
            SlotFn<R> fn
        ) {
            try {
                final TerminalSession session = sessionManager.get(sessionId);
                final SlotCommandStatus slotCommandStatus = fn.call(request, session.terminalController());
                responseObserver.onNext(slotCommandStatus);
                responseObserver.onCompleted();
            } catch (InvalidSessionRequestException | TerminalController.TerminalControllerResetException e) {
                LOG.warn("Exception while configureSlot ", e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asRuntimeException());
            }
        }
    }
}
