package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ReceivedDataStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.SendSlotDataMessage;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsRequest;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotCommandStatus;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.ContextSpec;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

public class LzyKharon {

    private static final Logger LOG = LogManager.getLogger(LzyKharon.class);
    private static final Options options = new Options();
    public static String host;
    public static int port;
    public static int servantPort;
    public static URI serverAddress;
    public static URI whiteboardAddress;
    public static URI snapshotAddress;

    static {
        options.addOption(new Option("h", "host", true, "Kharon host name"));
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("s", "servant-proxy-port", true, "gRPC servant port setting"));
        options.addOption(
            new Option("z", "lzy-server-address", true, "Lzy server address [host:port]"));
        options.addOption(
            new Option("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]"));
        options.addOption(
            new Option("lsa", "lzy-snapshot-address", true, "Lzy snapshot address [host:port]"));
    }

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final WbApiGrpc.WbApiBlockingStub whiteboard;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final TerminalSessionManager terminalManager;
    private final DataCarrier dataCarrier = new DataCarrier();
    private final ServantConnectionManager connectionManager = new ServantConnectionManager();
    private final Server kharonServer;
    private final Server kharonServantProxy;

    public LzyKharon(URI serverUri, URI whiteboardUri, URI snapshotUri, String host, int port,
                     int servantProxyPort) throws URISyntaxException {
        final ManagedChannel serverChannel = ChannelBuilder
            .forAddress(serverUri.getHost(), serverUri.getPort())
            .usePlaintext()
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        final ManagedChannel whiteboardChannel = ChannelBuilder
            .forAddress(whiteboardUri.getHost(), whiteboardUri.getPort())
            .usePlaintext()
            .build();
        whiteboard = WbApiGrpc.newBlockingStub(whiteboardChannel);

        final ManagedChannel snapshotChannel = ChannelBuilder
            .forAddress(snapshotUri.getHost(), snapshotUri.getPort())
            .usePlaintext()
            .build();
        snapshot = SnapshotApiGrpc.newBlockingStub(snapshotChannel);

        final URI servantProxyAddress = new URI("http", null, host, servantProxyPort, null, null,
            null);
        terminalManager = new TerminalSessionManager(server, servantProxyAddress);

        kharonServer = NettyServerBuilder.forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(
                ServerInterceptors.intercept(new KharonService(), new SessionIdInterceptor()))
            .addService(ServerInterceptors.intercept(new SnapshotService(), new SessionIdInterceptor()))
            .addService(ServerInterceptors.intercept(new WhiteboardService(), new SessionIdInterceptor()))
            .addService(ServerInterceptors.intercept(new ServerService(), new SessionIdInterceptor()))
            .build();
        kharonServantProxy = NettyServerBuilder.forPort(servantProxyPort)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors
                .intercept(new KharonServantProxyService(), new SessionIdInterceptor()))
            .build();
    }

    public static void main(String[] args)
        throws IOException, InterruptedException, URISyntaxException {
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
        host = parse.getOptionValue('h', "localhost");
        port = Integer.parseInt(parse.getOptionValue('p', "8899"));
        servantPort = Integer.parseInt(parse.getOptionValue('s', "8900"));
        serverAddress = URI.create(parse.getOptionValue('z', "http://localhost:8888"));
        whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));
        snapshotAddress = URI
            .create(parse.getOptionValue("lzy-snapshot-address", "http://localhost:8999"));

        final LzyKharon kharon = new LzyKharon(serverAddress, whiteboardAddress, snapshotAddress,
            host, port, servantPort);
        kharon.start();
        kharon.awaitTermination();
    }

    public void start() throws IOException {
        kharonServer.start();
        kharonServantProxy.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            kharonServer.shutdown();
            kharonServantProxy.shutdown();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        kharonServer.awaitTermination();
        kharonServantProxy.awaitTermination();
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
        public void prepareToSave(LzyWhiteboard.PrepareCommand request,
                                  StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            ProxyCall.exec(snapshot::prepareToSave, request, responseObserver);
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
        public StreamObserver<TerminalState> attachTerminal(
            StreamObserver<TerminalCommand> responseObserver) {
            LOG.info("Kharon::attachTerminal");
            return terminalManager.createSession(responseObserver);
        }

        @Override
        public StreamObserver<SendSlotDataMessage> writeToInputSlot(
            StreamObserver<ReceivedDataStatus> responseObserver) {
            LOG.info("Kharon::writeToInputSlot");
            return dataCarrier.connectTerminalConnection(responseObserver);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request,
                                   StreamObserver<Servant.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI slotUri = URI.create(request.getSlotUri());
            String slotHost = slotUri.getHost();
            if (slotHost.equals("host.docker.internal")) {
                slotHost = "localhost";
            }

            URI servantUri = null;
            try {
                servantUri = new URI(null, null, slotHost, slotUri.getPort(), null, null, null);
                final LzyServantGrpc.LzyServantBlockingStub servant = connectionManager
                    .getOrCreate(servantUri);
                LOG.info("Created connection to servant " + servantUri);
                final Iterator<Servant.Message> messageIterator = servant.openOutputSlot(request);
                while (messageIterator.hasNext()) {
                    responseObserver.onNext(messageIterator.next());
                }
                responseObserver.onCompleted();
                LOG.info("openOutputSlot completed for " + servantUri);
            } catch (Exception e) {
                LOG.error("Exception while openOutputSlot " + e);
                responseObserver.onError(e);
            } finally {
                if (servantUri != null) {
                    connectionManager.shutdownConnection(servantUri);
                }
            }
        }
    }

    private class ServerService extends LzyServerGrpc.LzyServerImplBase {

        @Override
        public void publish(Lzy.PublishRequest request,
                            StreamObserver<Operations.RegisteredZygote> responseObserver) {
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
        public void start(Tasks.TaskSpec request,
                          StreamObserver<Servant.ExecutionProgress> responseObserver) {
            LOG.info("Kharon::start " + JsonUtils.printRequest(request));
            try {
                final Iterator<Servant.ExecutionProgress> start = server.start(request);
                while (start.hasNext()) {
                    responseObserver.onNext(start.next());
                }
                LOG.info(
                    "Kharon::start user task completed " + request.getAuth().getUser().getUserId());
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
        public void prepare(ContextSpec request, StreamObserver<ContextProgress> responseObserver) {
            try {
                final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
                LOG.info("KharonServantProxyService sessionId = " + session.sessionId()
                    + "::prepare " + JsonUtils.printRequest(request));
                session.setExecutionProgress(responseObserver);
                Context.current().addListener(context -> {
                    LOG.info("Execution terminated from server");
                    session.close();
                }, Runnable::run);
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request,
                                   StreamObserver<Servant.Message> responseObserver) {
            try {
                final TerminalSession session = terminalManager
                    .getTerminalSessionFromSlotUri(request.getSlotUri());
                LOG.info("KharonServantProxyService sessionId = " + session.sessionId()
                    + "::openOutputSlot " + JsonUtils.printRequest(request));
                LOG.info("carryTerminalSlotContent: slot " + request.getSlot());
                dataCarrier.openServantConnection(URI.create(request.getSlotUri()),
                    responseObserver);
                session.configureSlot(Servant.SlotCommand.newBuilder()
                    .setSlot(request.getSlot())
                    .setConnect(Servant.ConnectSlotCommand.newBuilder()
                        .setSlotUri(request.getSlotUri())
                        .build())
                    .build());
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
        }

        @Override
        public void configureSlot(Servant.SlotCommand request,
                                  StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            try {
                final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
                final SlotCommandStatus slotCommandStatus = session.configureSlot(request);
                responseObserver.onNext(slotCommandStatus);
                responseObserver.onCompleted();
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
        }
    }
}
