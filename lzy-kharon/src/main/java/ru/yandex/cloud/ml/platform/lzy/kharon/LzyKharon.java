package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.utils.SessionIdInterceptor;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ReceivedDataStatus;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.SendSlotDataMessage;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsRequest;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsApi.SlotCommandStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyKharon;
import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.*;

public class LzyKharon {

    private static final Logger LOG = LogManager.getLogger(LzyKharon.class);
    private static final Options options = new Options();

    static {
        options.addRequiredOption("h", "host", true, "Kharon host name");
        options.addOption("e", "external", true, "Kharon external host");
        options.addRequiredOption("p", "port", true, "gRPC port setting");
        options.addRequiredOption("s", "servant-proxy-port", true, "gRPC servant port setting");
        options.addRequiredOption("fs", "servantfs-proxy-port", true, "gRPC servant fs port setting");
        options.addRequiredOption("z", "lzy-server-address", true, "Lzy server address [host:port]");
        options.addRequiredOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
        options.addRequiredOption("lsa", "lzy-snapshot-address", true, "Lzy snapshot address [host:port]");
    }

    private final URI address;
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final WbApiGrpc.WbApiBlockingStub whiteboard;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final TerminalSessionManager terminalManager;
    private final DataCarrier dataCarrier = new DataCarrier();
    private final ServantConnectionManager connectionManager = new ServantConnectionManager();
    private final Server kharonServer;
    private final Server kharonServantProxy;
    private final Server kharonServantFsProxy;
    private final ManagedChannel serverChannel;
    private final ManagedChannel whiteboardChannel;
    private final ManagedChannel snapshotChannel;

    public LzyKharon(URI serverUri, URI whiteboardUri, URI snapshotUri, String host, int port,
                     int servantProxyPort, int servantFsProxyPort) throws URISyntaxException {
        address = new URI(LzyKharon.scheme(), null, host, port, null, null, null);
        serverChannel = ChannelBuilder
            .forAddress(serverUri.getHost(), serverUri.getPort())
            .usePlaintext()
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        whiteboardChannel = ChannelBuilder
            .forAddress(whiteboardUri.getHost(), whiteboardUri.getPort())
            .usePlaintext()
            .build();
        whiteboard = WbApiGrpc.newBlockingStub(whiteboardChannel);

        snapshotChannel = ChannelBuilder
            .forAddress(snapshotUri.getHost(), snapshotUri.getPort())
            .usePlaintext()
            .build();
        snapshot = SnapshotApiGrpc.newBlockingStub(snapshotChannel);

        final URI servantProxyAddress = new URI(LzyServant.scheme(), null, host, servantProxyPort, null, null, null);
        final URI servantFsProxyAddress = new URI(LzyFs.scheme(), null, host, servantFsProxyPort, null, null, null);

        terminalManager = new TerminalSessionManager(server, servantProxyAddress, servantFsProxyAddress);

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
            .addService(ServerInterceptors.intercept(new KharonServantProxyService(), new SessionIdInterceptor()))
            .build();

        kharonServantFsProxy = NettyServerBuilder.forPort(servantFsProxyPort)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(new KharonServantFsProxyService(), new SessionIdInterceptor()))
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
        final String host = parse.getOptionValue('h', "localhost");
        final int port = Integer.parseInt(parse.getOptionValue('p', "8899"));
        final int servantPort = Integer.parseInt(parse.getOptionValue('s', "8900"));
        final int servantFsPort = parse.hasOption("fs") ? Integer.parseInt(parse.getOptionValue("fs"))
                : servantPort + 1;
        final URI serverAddress = URI.create(parse.getOptionValue('z', "http://localhost:8888"));
        final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));
        final URI snapshotAddress = URI.create(parse.getOptionValue("lzy-snapshot-address", "http://localhost:8999"));

        final LzyKharon kharon = new LzyKharon(serverAddress, whiteboardAddress, snapshotAddress,
            host, port, servantPort, servantFsPort);
        kharon.start();
        kharon.awaitTermination();
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
        public StreamObserver<TerminalState> attachTerminal(StreamObserver<TerminalCommand> responseObserver) {
            LOG.info("Kharon::attachTerminal");
            return terminalManager.createSession(responseObserver);
        }

        @Override
        public StreamObserver<SendSlotDataMessage> writeToInputSlot(StreamObserver<ReceivedDataStatus> response) {
            LOG.info("Kharon::writeToInputSlot");
            return dataCarrier.connectTerminalConnection(response);
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI connectUri = URI.create(request.getSlotUri());
            final Optional<String> uri =
                URLEncodedUtils.parse(connectUri, StandardCharsets.UTF_8)
                    .stream()
                    .filter(t -> t.getName().equals("slot_uri"))
                    .findFirst()
                    .map(NameValuePair::getValue);
            if (uri.isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad uri").asException());
                return;
            }
            final URI slotUri = URI.create(uri.get());

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
        public void start(Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> responseObserver) {
            LOG.info("Kharon::start " + JsonUtils.printRequest(request));
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
                final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
                LOG.info("KharonServantProxyService sessionId = " + session.sessionId()
                    + "::prepare " + JsonUtils.printRequest(request));
                session.setServantProgress(responseObserver);
                Context.current().addListener(context -> {
                    LOG.info("Execution terminated from server");
                    session.close();
                }, Runnable::run);
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
        }

    }

    private class KharonServantFsProxyService extends LzyFsGrpc.LzyFsImplBase {

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request,
                                   StreamObserver<LzyFsApi.Message> responseObserver) {
            try {
                final TerminalSession session = terminalManager
                    .getTerminalSessionFromSlotUri(request.getSlotUri());
                LOG.info("KharonServantFsProxyService sessionId = " + session.sessionId()
                    + "::openOutputSlot " + JsonUtils.printRequest(request));
                dataCarrier.openServantConnection(URI.create(request.getSlotUri()), responseObserver);

                Path path = Path.of(URI.create(request.getSlotUri()).getPath());
                String tid = path.getName(0).toString();
                String slot = Path.of("/", path.subpath(1, path.getNameCount()).toString()).toString();

                session.configureSlot(LzyFsApi.SlotCommand.newBuilder()
                    .setSlot(slot)
                    .setTid(tid)
                    .setConnect(LzyFsApi.ConnectSlotCommand.newBuilder()
                        .setSlotUri(request.getSlotUri())
                        .build())
                    .build());
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            }
        }

        @Override
        public void configureSlot(LzyFsApi.SlotCommand request,
                                  StreamObserver<LzyFsApi.SlotCommandStatus> responseObserver) {
            try {
                final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
                if (request.hasConnect()) {
                    URI uri = URI.create(request.getConnect().getSlotUri());
                    if (SlotS3.match(uri) || SlotAzure.match(uri)) {
                        ProxyCall.exec(session::configureSlot, request, responseObserver);
                        return;
                    }
                    URI builtURI = new URIBuilder()
                        .setScheme(LzyKharon.scheme())
                        .setHost(address.getHost())
                        .setPort(address.getPort())
                        .addParameter("slot_uri", request.getConnect().getSlotUri())
                        .build();
                    request = LzyFsApi.SlotCommand.newBuilder()
                        .mergeFrom(request)
                        .setConnect(LzyFsApi.ConnectSlotCommand.newBuilder()
                            .setSlotUri(builtURI.toString()).build())
                        .build();
                }
                final SlotCommandStatus slotCommandStatus = session.configureSlot(request);
                responseObserver.onNext(slotCommandStatus);
                responseObserver.onCompleted();
            } catch (InvalidSessionRequestException e) {
                responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            } catch (URISyntaxException e) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Invalid servant uri")
                    .asRuntimeException());
            }
        }
    }
}
