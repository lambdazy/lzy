package ru.yandex.cloud.ml.platform.lzy.kharon;

import com.google.common.net.HostAndPort;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConfig;
import ru.yandex.cloud.ml.platform.lzy.kharon.TerminalController.TerminalControllerResetException;
import ru.yandex.cloud.ml.platform.lzy.kharon.workflow.WorkflowService;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.utils.SessionIdInterceptor;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ReceivedDataStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.SendSlotDataMessage;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsRequest;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsApi.SlotCommandStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyKharon;
import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.*;

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
    }

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final WbApiGrpc.WbApiBlockingStub whiteboard;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final TerminalSessionManager sessionManager;
    private final DataCarrier dataCarrier = new DataCarrier();
    private final ServantConnectionManager connectionManager = new ServantConnectionManager();
    private final ServerControllerFactory serverControllerFactory;
    private final Server kharonServer;
    private final Server kharonServantProxy;
    private final Server kharonServantFsProxy;
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

        var servantProxyAddress = new URI(LzyServant.scheme(), null, selfAddress.getHost(), config.servantProxyPort(),
            null, null, null);
        var servantProxyFsAddress = new URI(LzyFs.scheme(), null, selfAddress.getHost(), config.servantProxyFsPort(),
            null, null, null);

        sessionManager = new TerminalSessionManager();
        uriResolver = new UriResolver(externalAddress, servantProxyFsAddress);
        serverControllerFactory = new ServerControllerFactory(server, uriResolver, servantProxyAddress,
                servantProxyFsAddress);

        var sessionIdInterceptor = new SessionIdInterceptor();

        var kharonServerBuilder = NettyServerBuilder.forPort(selfAddress.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIMEOUT_SECS,  TimeUnit.SECONDS)
            .addService(ServerInterceptors.intercept(new KharonService(), sessionIdInterceptor))
            .addService(ServerInterceptors.intercept(new SnapshotService(), sessionIdInterceptor))
            .addService(ServerInterceptors.intercept(new WhiteboardService(), sessionIdInterceptor))
            .addService(ServerInterceptors.intercept(new ServerService(), sessionIdInterceptor));

        if (config.workflow() != null && config.workflow().serverAddress() != null) {
            var authInterceptor = new AuthServerInterceptor(
                    new AuthenticateServiceGrpcClient(GrpcConfig.from(config.iamAddress())));
            var workflowService = ctx.getBean(WorkflowService.class);
            kharonServerBuilder.addService(ServerInterceptors.intercept(workflowService, authInterceptor));
        }
        kharonServer = kharonServerBuilder.build();

        kharonServantProxy = NettyServerBuilder.forPort(config.servantProxyPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIMEOUT_SECS,  TimeUnit.SECONDS)
            .addService(ServerInterceptors.intercept(new KharonServantProxyService(), sessionIdInterceptor))
            .build();

        kharonServantFsProxy = NettyServerBuilder.forPort(config.servantProxyFsPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            //.keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIMEOUT_SECS,  TimeUnit.SECONDS)
            .addService(ServerInterceptors.intercept(new KharonServantFsProxyService(), sessionIdInterceptor))
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
            final URI serverAddress = URI.create(parse.getOptionValue('z', "http://localhost:8888"));
            final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));
            final URI snapshotAddress = URI.create(parse.getOptionValue("lzy-snapshot-address", "http://localhost:8999"));

            p.put("kharon.address", host + ":" + port);
            p.put("kharon.external-host", externalHost);
            p.put("kharon.server-address", serverAddress.getHost() + ":" + serverAddress.getPort());
            p.put("kharon.whiteboard-address", whiteboardAddress.getHost() + ":" + whiteboardAddress.getPort());
            p.put("kharon.snapshot-address", snapshotAddress.getHost() + ":" + snapshotAddress.getPort());
            p.put("kharon.servant-proxy-port", servantPort);
            p.put("kharon.servant-proxy-fs-port", servantFsPort);

            // p.put("kharon.workflow", null);
        }

        try (ApplicationContext context = ApplicationContext.run(p)) {
            final LzyKharon kharon = new LzyKharon(context);

            kharon.start();
            kharon.awaitTermination();
        }
    }

    public void start() throws IOException {
        kharonServer.start();
        kharonServantProxy.start();
        kharonServantFsProxy.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            kharonServer.shutdown();
            kharonServantProxy.shutdown();
            kharonServantFsProxy.shutdown();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        kharonServer.awaitTermination();
        kharonServantProxy.awaitTermination();
        kharonServantFsProxy.awaitTermination();
    }

    public void close() {
        serverChannel.shutdownNow();
        whiteboardChannel.shutdownNow();
        snapshotChannel.shutdownNow();
        kharonServer.shutdownNow();
        kharonServantProxy.shutdownNow();
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
        public StreamObserver<Kharon.ServerCommand> attachTerminal(StreamObserver<TerminalCommand> responseObserver) {
            LOG.info("Kharon::attachTerminal");
            final String sessionId = "terminal_" + UUID.randomUUID();
            final TerminalSession session = sessionManager.createSession(
                sessionId,
                new TerminalController(responseObserver),
                serverControllerFactory
            );
            Context.current().addListener(context -> {
                session.onTerminalDisconnect();
                sessionManager.deleteSession(sessionId);
            }, Runnable::run);
            return session.serverCommandHandler();
        }

        @Override
        public StreamObserver<SendSlotDataMessage> writeToInputSlot(StreamObserver<ReceivedDataStatus> response) {
            LOG.info("Kharon::writeToInputSlot");
            return dataCarrier.connectTerminalConnection(response);
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI slotUri = UriResolver.parseSlotUri(URI.create(request.getSlotUri()));
            if (slotUri == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad uri").asException());
                return;
            }

            LzyFsApi.SlotRequest newRequest = LzyFsApi.SlotRequest.newBuilder()
                .mergeFrom(request)
                .setSlotUri(slotUri.toString())
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
        public void channel(Channels.ChannelCommand request,
                            StreamObserver<Channels.ChannelStatus> responseObserver) {
            LOG.info("Kharon::channel " + JsonUtils.printRequest(request));
            ProxyCall.exec(server::channel, request, responseObserver);
        }

        @Override
        public void tasksStatus(IAM.Auth request,
                                StreamObserver<Tasks.TasksList> responseObserver) {
            ProxyCall.exec(server::tasksStatus, request, responseObserver);
        }

        @Override
        public void channelsStatus(IAM.Auth request,
                                   StreamObserver<Channels.ChannelStatusList> responseObserver) {
            ProxyCall.exec(server::channelsStatus, request, responseObserver);
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


    private class KharonServantProxyService extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            try {
                final TerminalSession session = sessionManager.getSessionFromGrpcContext();
                final String sessionId = session.sessionId();
                LOG.info("KharonServantProxyService sessionId = " + sessionId
                    + "::start " + JsonUtils.printRequest(request));
                session.setServerStream(responseObserver);
                Context.current().addListener(context -> {
                    LOG.info("Execution terminated from server");
                    session.onServerDisconnect();
                    sessionManager.deleteSession(sessionId);
                }, Runnable::run);
            } catch (InvalidSessionRequestException | ServerController.ServerControllerResetException e) {
                LOG.warn("Exception while start ", e);
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException().initCause(e));
            }
        }

    }

    private class KharonServantFsProxyService extends LzyFsGrpc.LzyFsImplBase {

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request,
                                   StreamObserver<LzyFsApi.Message> responseObserver) {
            try {
                final TerminalSession session = sessionManager.getSessionFromSlotUri(request.getSlotUri());
                LOG.info("KharonServantFsProxyService sessionId = " + session.sessionId()
                    + "::openOutputSlot " + JsonUtils.printRequest(request));
                dataCarrier.openServantConnection(URI.create(request.getSlotUri()), responseObserver);

                final URI slotUri = URI.create(request.getSlotUri());
                final String tid = UriResolver.parseTidFromSlotUri(slotUri);
                final String slotName = UriResolver.parseSlotNameFromSlotUri(slotUri);

                session.terminalController().configureSlot(LzyFsApi.SlotCommand.newBuilder()
                    .setSlot(slotName)
                    .setTid(tid)
                    .setConnect(LzyFsApi.ConnectSlotCommand.newBuilder()
                        .setSlotUri(request.getSlotUri())
                        .build())
                    .build());
            } catch (InvalidSessionRequestException | TerminalControllerResetException e) {
                LOG.warn("Exception while openOutputSlot ", e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asRuntimeException());
            }
        }

        @Override
        public void configureSlot(LzyFsApi.SlotCommand request,
                                  StreamObserver<LzyFsApi.SlotCommandStatus> responseObserver) {
            try {
                final TerminalSession session = sessionManager.getSessionFromGrpcContext();
                if (request.hasConnect()) {
                    final URI uri = URI.create(request.getConnect().getSlotUri());
                    if (!SlotS3.match(uri) && !SlotAzure.match(uri)) {
                        final URI convertedToKharonUri = uriResolver.convertToKharonWithSlotUri(uri);
                        request = LzyFsApi.SlotCommand.newBuilder()
                            .mergeFrom(request)
                            .setConnect(LzyFsApi.ConnectSlotCommand.newBuilder()
                                .setSlotUri(convertedToKharonUri.toString()).build())
                            .build();
                    }
                }
                final SlotCommandStatus slotCommandStatus = session.terminalController().configureSlot(request);
                responseObserver.onNext(slotCommandStatus);
                responseObserver.onCompleted();
            } catch (InvalidSessionRequestException | TerminalControllerResetException e) {
                LOG.warn("Exception while configureSlot ", e);
                responseObserver.onError(Status.NOT_FOUND.withCause(e).asRuntimeException());
            } catch (URISyntaxException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Invalid servant uri")
                    .asRuntimeException());
            }
        }
    }
}
