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
import java.util.UUID;

public class LzyKharon {
    private static final Logger LOG = LogManager.getLogger(LzyKharon.class);
    private final LzyServerGrpc.LzyServerBlockingStub server;

    private final TerminalSessionManager terminalManager;

    private static final Options options = new Options();
    static {
        options.addOption(new Option("h", "host", true, "Kharon host name"));
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("s", "servant-proxy-port", true, "gRPC servant port setting"));
        options.addOption(new Option("z", "lzy-server-address", true, "Lzy server address [host:port]"));
    }

    public static String host;
    public static int port;
    public static int servantPort;
    public static URI serverAddress;

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

        final LzyKharon kharon = new LzyKharon(serverAddress, host, port, servantPort);
        kharon.start();
        kharon.awaitTermination();
    }


    public LzyKharon(URI serverUri, String host, int port, int servantProxyPort) throws URISyntaxException {
        final ManagedChannel serverChannel = ManagedChannelBuilder
            .forAddress(serverUri.getHost(), serverUri.getPort())
            .usePlaintext()
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);
        final URI servantProxyAddress = new URI("http", null, host, servantProxyPort, null, null, null);
        terminalManager = new TerminalSessionManager(server, servantProxyAddress);

        kharonServer = ServerBuilder
            .forPort(port)
            .addService(new KharonService())
            .build();
        kharonServantProxy = ServerBuilder
            .forPort(servantProxyPort)
            .addService(new KharonServantProxyService())
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
            final TerminalSession terminalSession = getTerminalSession();
            return terminalSession.initDataTransfer(responseObserver);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            LOG.info("Kharon::openOutputSlot from Terminal " + JsonUtils.printRequest(request));
            final URI slotUri = URI.create(request.getSlotUri());
            String slotHost = slotUri.getHost();
            if (slotHost.equals("host.docker.internal")) {
                slotHost = "localhost";
            }

            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(slotHost, slotUri.getPort())
                .usePlaintext()
                .build();
            final Iterator<Servant.Message> messageIterator = LzyServantGrpc.newBlockingStub(channel).openOutputSlot(request);
            while (messageIterator.hasNext()) {
                responseObserver.onNext(messageIterator.next());
            }
            responseObserver.onCompleted();
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
    }

    private class KharonServantProxyService extends LzyServantGrpc.LzyServantImplBase {
        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            LOG.info("KharonServantProxyService::execute " + JsonUtils.printRequest(request));
            final TerminalSession session = getTerminalSession();
            session.setExecutionProgress(responseObserver);
            Context.current().addListener(context -> {
                LOG.info("Execution terminated from server ");
                session.close();
            }, Runnable::run);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            LOG.info("KharonServantProxyService::openOutputSlot " + JsonUtils.printRequest(request));
            final TerminalSession session = getTerminalSession();
            session.carryTerminalSlotContent(request, responseObserver);
        }

        @Override
        public void configureSlot(Servant.SlotCommand request, StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            final TerminalSession session = getTerminalSession();
            final Servant.SlotCommandStatus slotCommandStatus = session.configureSlot(request);
            responseObserver.onNext(slotCommandStatus);
            responseObserver.onCompleted();
        }
    }

    private final UUID sessionId = UUID.randomUUID();
    private TerminalSession getTerminalSession() {
        // TODO(d-kruchinin): multi-session
        // final UUID sessionId = UUID.fromString(Constant.SESSION_ID_CTX_KEY.get());
        return terminalManager.getSession(sessionId);
    }
}
