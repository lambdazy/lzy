package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;


public class WhiteboardApi {
    private static final Logger LOG = LogManager.getLogger(WhiteboardApi.class);

    private static final Options options = new Options();

    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
    }

    private static final String LZY_WHITEBOARD_HOST_ENV = "LZY_WHITEBOARD_HOST";
    private static final String DEFAULT_LZY_WHITEBOARD_LOCALHOST = "http://localhost";
    private static URI serverURI;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-whiteboard", options);
            System.exit(-1);
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "8999"));
        final String lzyWhiteboardHost;
        if (System.getenv().containsKey(LZY_WHITEBOARD_HOST_ENV)) {
            lzyWhiteboardHost = "http://" + System.getenv(LZY_WHITEBOARD_HOST_ENV);
        } else {
            lzyWhiteboardHost = DEFAULT_LZY_WHITEBOARD_LOCALHOST;
        }
        serverURI = URI.create(lzyWhiteboardHost + ":" + port);
        Impl impl = new Impl();
        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(impl);
        final Server server = builder.build();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            server.shutdown();
        }));
        server.awaitTermination();
    }

    public static class Impl extends WhiteboardApiGrpc.WhiteboardApiImplBase {
        @Override
        public void prepareToSave(LzyWhiteboard.PrepareCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            System.out.println("WhiteboardApi::prepareToSave with opName " + request.getOpName() + " and " + request.getSlot().getName());
            LOG.info("WhiteboardApi::prepareToSave with opName " + request.getOpName() + " and " + request.getSlot().getName());
            final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                    .newBuilder()
                    .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                    .build();
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void commit(LzyWhiteboard.CommitCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            System.out.println("WhiteboardApi::commit with opName " + request.getOpName() + " and " + request.getSlot().getName());
            LOG.info("WhiteboardApi::commit with opName " + request.getOpName() + " and " + request.getSlot().getName());
            final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                    .newBuilder()
                    .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                    .build();
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void addDependencies(LzyWhiteboard.DependenciesCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            LOG.info("WhiteboardApi::addDependencies with opName " + request.getOpName());
            final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                    .newBuilder()
                    .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                    .build();
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void getWhiteboard(LzyWhiteboard.WhiteboardCommand request,
                                  StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
            System.out.println("WhiteboardApi::getWhiteboard with opName ");
            final LzyWhiteboard.Whiteboard whiteboard = LzyWhiteboard.Whiteboard
                    .newBuilder()
                    .build();
            responseObserver.onNext(whiteboard);
            responseObserver.onCompleted();
        }
    }
}
