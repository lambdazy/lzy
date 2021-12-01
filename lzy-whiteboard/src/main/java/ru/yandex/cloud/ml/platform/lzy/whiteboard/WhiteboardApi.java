package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WhiteboardApi {
    // IN_PROGRESS --> started saving data
    // FINISHED --> finished saving data
    enum Status {
        IN_PROGRESS,
        FINISHED
    }
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
        private static class StorageBinding {
            private final String fieldName;
            private final Slot slot;
            private final URI storageUri;
            private Status status;
            private boolean isEmpty = true;

            private StorageBinding(String fieldName, Slot slot, String storageUri) {
                this.fieldName = fieldName;
                this.slot = slot;
                this.storageUri = URI.create(storageUri);
                status = Status.IN_PROGRESS;
            }

            public void setStatus(Status newStatus) {
                status = newStatus;
            }

            public static LzyWhiteboard.StorageBinding to(StorageBinding bindings) {
                return LzyWhiteboard.StorageBinding
                        .newBuilder()
                        .setFieldName(bindings.fieldName)
                        .setStorageUri(bindings.storageUri.toString())
                        .build();
            }

            public void setEmpty(boolean empty) {
                isEmpty = empty;
            }
        }

        private static class Dependency {
            private final String fieldName;
            private final ArrayList<String> dependencies = new ArrayList<>();

            private Dependency(String fieldName, List<String> deps) {
                this.fieldName = fieldName;
                this.dependencies.addAll(deps);
            }

            public static LzyWhiteboard.Relation to(Dependency dep) {
                return LzyWhiteboard.Relation
                        .newBuilder()
                        .setFieldName(dep.fieldName)
                        .addAllDependencies(dep.dependencies)
                        .build();
            }
        }

        private final ConcurrentHashMap<URI, Set<StorageBinding>> storageBindings = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<URI, Set<Dependency>> dependencies = new ConcurrentHashMap<>();


        @Override
        public void prepareToSave(LzyWhiteboard.PrepareCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            LOG.info("WhiteboardApi::prepareToSave invoked with opName " + request.getFieldName() +
                    ", slotName " + request.getSlot().getName() +
                    ", whiteboard id " + request.getWbId()
            );
            storageBindings.putIfAbsent(URI.create(request.getWbId()), new HashSet<>());
            storageBindings.computeIfPresent(URI.create(request.getWbId()),
                    (k, v) -> {
                        v.add(new StorageBinding(request.getFieldName(), gRPCConverter.from(request.getSlot()), request.getStorageUri()));
                        return v;
                    });
            final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                    .newBuilder()
                    .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                    .build();
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void commit(LzyWhiteboard.CommitCommand request, StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
            LOG.info("WhiteboardApi::commit invoked with opName " + request.getFieldName() +
                    ", slotName " + request.getSlot().getName() +
                    ", whiteboard id " + request.getWbId()
            );
            storageBindings.computeIfPresent(URI.create(request.getWbId()),
                    (k, v) -> {
                        for (var sb : v) {
                            if (sb.slot.name().equals(request.getSlot().getName())) {
                                sb.setStatus(Status.FINISHED);
                                sb.setEmpty(request.getEmpty());
                            }
                        }
                        return v;
                    });
            if (request.hasDependencies()) {
                dependencies.putIfAbsent(URI.create(request.getWbId()), new HashSet<>());
                dependencies.computeIfPresent(URI.create(request.getWbId()),
                        (k, v) -> {
                            List<String> deps = request.getDependencies().getDependenciesList();
                            for (var dep : v) {
                                if (dep.fieldName.equals(request.getFieldName())) {
                                    return v;
                                }
                            }
                            v.add(new Dependency(request.getFieldName(), deps));
                            return v;
                        });
            }
            final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
                    .newBuilder()
                    .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
                    .build();
            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void getWhiteboard(LzyWhiteboard.GetWhiteboardCommand request,
                                  StreamObserver<LzyWhiteboard.Whiteboard> responseObserver) {
            LOG.info("WhiteboardApi::getWhiteboard invoked with whiteboard id " + request.getWbId());
            List<LzyWhiteboard.StorageBinding> bindings = new ArrayList<>();
            for (var entry : storageBindings.get(URI.create(request.getWbId()))) {
                if (entry.slot.direction().equals(Slot.Direction.OUTPUT)) {
                    if (!entry.isEmpty) {
                        bindings.add(StorageBinding.to(entry));
                    }
                }
            }
            List<LzyWhiteboard.Relation> relations = new ArrayList<>();
            for (var entry : dependencies.get(URI.create(request.getWbId()))) {
                relations.add(Dependency.to(entry));
            }
            final LzyWhiteboard.Whiteboard whiteboard = LzyWhiteboard.Whiteboard
                    .newBuilder()
                    .addAllStorageBindings(bindings)
                    .addAllRelations(relations)
                    .build();
            responseObserver.onNext(whiteboard);
            responseObserver.onCompleted();
        }

        @Override
        public void getWhiteboardId(LzyWhiteboard.GetWhiteboardIdCommand request,
                                    StreamObserver<LzyWhiteboard.WhiteboardId> responseObserver) {
            LOG.info("WhiteboardApi::getWhiteboardId invoked with user " + request.getUserCredentials().getUserId());
            String id = request.getUserCredentials().getUserId() + "/" + request.getCustomId();
            int i = 0;
            while (storageBindings.containsKey(URI.create(id + "/" + i))) {
                i++;
            }
            id = id + "/" + i;
            final LzyWhiteboard.WhiteboardId whiteboardId = LzyWhiteboard.WhiteboardId
                    .newBuilder()
                    .setWbId(id)
                    .build();
            responseObserver.onNext(whiteboardId);
            responseObserver.onCompleted();
        }
    }
}
