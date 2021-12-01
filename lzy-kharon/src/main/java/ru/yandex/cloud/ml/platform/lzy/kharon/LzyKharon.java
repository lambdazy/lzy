package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

public class LzyKharon {
    private static final Logger LOG = LogManager.getLogger(LzyKharon.class);
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final WhiteboardApiGrpc.WhiteboardApiBlockingStub whiteboard;

    private final TerminalSessionManager terminalManager;
    private final DataCarrier dataCarrier = new DataCarrier();
    private final ServantConnectionManager connectionManager = new ServantConnectionManager();

    private static final Options options = new Options();
    static {
        options.addOption(new Option("h", "host", true, "Kharon host name"));
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("s", "servant-proxy-port", true, "gRPC servant port setting"));
        options.addOption(new Option("z", "lzy-server-address", true, "Lzy server address [host:port]"));
        options.addOption(new Option("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]"));
    }

    public static String host;
    public static int port;
    public static int servantPort;
    public static URI serverAddress;
    public static URI whiteboardAddress;

    private final Server kharonServer;
    private final Server kharonServantProxy;

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
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

        final LzyKharon kharon = new LzyKharon(serverAddress, whiteboardAddress, host, port, servantPort);
        kharon.start();
        kharon.awaitTermination();
    }


    public LzyKharon(URI serverUri, URI whiteboardUri, String host, int port, int servantProxyPort) throws URISyntaxException {
        final ManagedChannel serverChannel = ManagedChannelBuilder
            .forAddress(serverUri.getHost(), serverUri.getPort())
            .usePlaintext()
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);

        final ManagedChannel whiteboardChannel = ManagedChannelBuilder
                .forAddress(whiteboardUri.getHost(), whiteboardUri.getPort())
                .usePlaintext()
                .build();
        whiteboard = WhiteboardApiGrpc.newBlockingStub(whiteboardChannel);

        final URI servantProxyAddress = new URI("http", null, host, servantProxyPort, null, null, null);
        terminalManager = new TerminalSessionManager(server, servantProxyAddress);

        kharonServer = ServerBuilder
            .forPort(port)
            .addService(ServerInterceptors.intercept(new KharonService(), new SessionIdInterceptor()))
            .build();
        kharonServantProxy = ServerBuilder
            .forPort(servantProxyPort)
            .addService(ServerInterceptors.intercept(new KharonServantProxyService(), new SessionIdInterceptor()))
            .build();
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

    private class KharonService extends LzyKharonGrpc.LzyKharonImplBase {
        @Override
        public StreamObserver<TerminalState> attachTerminal(StreamObserver<TerminalCommand> responseObserver) {
            LOG.info("Kharon::attachTerminal");
            return terminalManager.createSession(responseObserver);
        }

        @Override
        public StreamObserver<SendSlotDataMessage> writeToInputSlot(StreamObserver<ReceivedDataStatus> responseObserver) {
            LOG.info("Kharon::writeToInputSlot");
            return dataCarrier.connectTerminalConnection(responseObserver);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI slotUri = URI.create(request.getSlotUri());
            String slotHost = slotUri.getHost();
            if (slotHost.equals("host.docker.internal")) {
                slotHost = "localhost";
            }

            URI servantUri = null;
            try {
                servantUri = new URI(null, null, slotHost, slotUri.getPort(), null, null, null);
                final LzyServantGrpc.LzyServantBlockingStub servant = connectionManager.getOrCreate(servantUri);
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

        @Override
        public void publish(Lzy.PublishRequest request, StreamObserver<Operations.RegisteredZygote> responseObserver) {
            final Operations.RegisteredZygote publish = server.publish(request);
            responseObserver.onNext(publish);
            responseObserver.onCompleted();
        }

        @Override
        public void zygotes(IAM.Auth request, StreamObserver<Operations.ZygoteList> responseObserver) {
            final Operations.ZygoteList zygotes = server.zygotes(request);
            responseObserver.onNext(zygotes);
            responseObserver.onCompleted();
        }

        @Override
        public void task(Tasks.TaskCommand request, StreamObserver<Tasks.TaskStatus> responseObserver) {
            final Tasks.TaskStatus task = server.task(request);
            responseObserver.onNext(task);
            responseObserver.onCompleted();
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            LOG.info("Kharon::start " + JsonUtils.printRequest(request));
            final Iterator<Servant.ExecutionProgress> start = server.start(request);
            while (start.hasNext()) {
                responseObserver.onNext(start.next());
            }
            LOG.info("Kharon::start user task completed " + request.getAuth().getUser().getUserId());
            responseObserver.onCompleted();
        }

        @Override
        public void channel(Channels.ChannelCommand request, StreamObserver<Channels.ChannelStatus> responseObserver) {
            LOG.info("Kharon::channel " + JsonUtils.printRequest(request));
            final Channels.ChannelStatus channel = server.channel(request);
            responseObserver.onNext(channel);
            responseObserver.onCompleted();
        }

        @Override
        public void tasksStatus(IAM.Auth request, StreamObserver<Tasks.TasksList> responseObserver) {
            final Tasks.TasksList tasksList = server.tasksStatus(request);
            responseObserver.onNext(tasksList);
            responseObserver.onCompleted();
        }

        @Override
        public void channelsStatus(IAM.Auth request, StreamObserver<Channels.ChannelStatusList> responseObserver) {
            responseObserver.onNext(server.channelsStatus(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request,
                                  StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
            responseObserver.onNext(whiteboard.getWhiteboard(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getWhiteboardId(LzyWhiteboard.GetWhiteboardIdCommand request,
                                  StreamObserver<LzyWhiteboard.WhiteboardId> responseObserver) {
            responseObserver.onNext(whiteboard.getWhiteboardId(request));
            responseObserver.onCompleted();
        }
    }

    private class KharonServantProxyService extends LzyServantGrpc.LzyServantImplBase {
        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
            LOG.info("KharonServantProxyService sessionId = " + session.getSessionId() +
                "::execute " + JsonUtils.printRequest(request));
            session.setExecutionProgress(responseObserver);
            Context.current().addListener(context -> {
                LOG.info("Execution terminated from server ");
                session.close();
            }, Runnable::run);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            final TerminalSession session = terminalManager.getTerminalSessionFromSlotUri(request.getSlotUri());
            LOG.info("KharonServantProxyService sessionId = " + session.getSessionId() +
                "::openOutputSlot " + JsonUtils.printRequest(request));
            LOG.info("carryTerminalSlotContent: slot " + request.getSlot());
            dataCarrier.openServantConnection(URI.create(request.getSlotUri()), responseObserver);
            session.configureSlot(Servant.SlotCommand.newBuilder()
                .setSlot(request.getSlot())
                .setConnect(Servant.ConnectSlotCommand.newBuilder()
                    .setSlotUri(request.getSlotUri())
                    .build())
                .build());
        }

        @Override
        public void configureSlot(Servant.SlotCommand request, StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            final TerminalSession session = terminalManager.getTerminalSessionFromGrpcContext();
            final Servant.SlotCommandStatus slotCommandStatus = session.configureSlot(request);
            responseObserver.onNext(slotCommandStatus);
            responseObserver.onCompleted();
        }
    }
}
