package ai.lzy.channelmanager.grpc;

import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static java.util.Objects.requireNonNull;

public class ChannelManagerMock {
    private static final Logger LOG = LogManager.getLogger(ChannelManagerMock.class);

    final int port;
    final Server server;
    final PublicServiceMock publicService;
    final PrivateServiceMock privateService;
    final Map<String, DirectChannelInfo> directChannels = new ConcurrentHashMap<>(); // channelId -> ...

    public ChannelManagerMock(HostAndPort address) {
        this.port = address.getPort();
        this.privateService = new PrivateServiceMock();
        this.publicService = new PublicServiceMock();
        server = newGrpcServer(address, null)
            .addService(ServerInterceptors.intercept(privateService))
            .addService(ServerInterceptors.intercept(publicService))
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() throws InterruptedException {
        for (var channel : directChannels.values()) {
            channel.close();
        }
        server.shutdown();
        server.awaitTermination();
    }

    @Nullable
    public DirectChannelInfo get(String channelId) {
        return directChannels.get(channelId);
    }

    public int port() {
        return port;
    }

    public void create(LCMPS.ChannelCreateRequest request, StreamObserver<LCMPS.ChannelCreateResponse> response) {
        privateService.create(request, response);
    }

    public void destroy(LCMPS.ChannelDestroyRequest request, StreamObserver<LCMPS.ChannelDestroyResponse> response) {
        privateService.destroy(request, response);
    }

    public void status(LCMPS.ChannelStatusRequest request, StreamObserver<LCMPS.ChannelStatus> response) {
        privateService.status(request, response);
    }

    public void statusAll(LCMPS.ChannelStatusAllRequest request, StreamObserver<LCMPS.ChannelStatusList> response) {
        privateService.statusAll(request, response);
    }

    public void bind(LCMS.BindRequest request, StreamObserver<LCMS.BindResponse> response) {
        publicService.bind(request, response);
    }

    public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> response) {
        publicService.unbind(request, response);
    }
    
    private static class Endpoint implements Closeable {
        private final LzyFsGrpc.LzyFsBlockingStub fs;
        private final ManagedChannel channel;
        private final SlotInstance slotInstance;

        private Endpoint(SlotInstance slotInstance) {
            this.slotInstance = slotInstance;
            channel = newGrpcChannel(slotInstance.uri().getHost(), slotInstance.uri().getPort(), LzyFsGrpc.SERVICE_NAME);
            fs = newBlockingClient(LzyFsGrpc.newBlockingStub(channel), "ChManMock", null);
        }

        LzyFsApi.SlotCommandStatus connect(SlotInstance to) {
            return fs.connectSlot(LzyFsApi.ConnectSlotRequest.newBuilder()
                .setFrom(ProtoConverter.toProto(slotInstance))
                .setTo(ProtoConverter.toProto(to))
                .build()
            );
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        void destroy() {
            fs.destroySlot(LzyFsApi.DestroySlotRequest.newBuilder()
                .setSlotInstance(ProtoConverter.toProto(slotInstance))
                .build());
        }

        @Override
        public void close() {
            channel.shutdown();
        }
    }

    public static class DirectChannelInfo implements Closeable {
        final AtomicReference<Endpoint> inputEndpoint = new AtomicReference<>(null);
        final AtomicReference<Endpoint> outputEndpoint = new AtomicReference<>(null);
        final AtomicReference<LCMS.BindRequest> inputSlot = new AtomicReference<>(null);
        public final AtomicReference<LCMS.BindRequest> outputSlot = new AtomicReference<>(null);

        boolean isCompleted() {
            return inputSlot.get() != null && outputSlot.get() != null;
        }

        @Override
        public void close() {
            final Endpoint inputEnd = inputEndpoint.get();
            if (inputEnd != null) {
                inputEnd.close();
            }
            final Endpoint outputEnd = outputEndpoint.get();
            if (outputEnd != null) {
                outputEnd.close();
            }
        }
    }
    
    private class PrivateServiceMock extends LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase {
        
        @Override
        public void create(LCMPS.ChannelCreateRequest request, StreamObserver<LCMPS.ChannelCreateResponse> response) {
            LOG.info("create {}", JsonUtils.printRequest(request));
            if (!request.getChannelSpec().hasDirect()) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("Not direct channel").asException());
                return;
            }

            var channelName = request.getChannelSpec().getChannelName();
            if (directChannels.putIfAbsent(channelName, new DirectChannelInfo()) != null) {
                response.onError(Status.ALREADY_EXISTS.asException());
                return;
            }

            response.onNext(
                LCMPS.ChannelCreateResponse.newBuilder()
                    .setChannelId(channelName)
                    .build()
            );
            response.onCompleted();
        }

        @Override
        public void destroy(LCMPS.ChannelDestroyRequest request, StreamObserver<LCMPS.ChannelDestroyResponse> response)
        {
            LOG.info("destroy {}", JsonUtils.printRequest(request));
            var channel = directChannels.remove(request.getChannelId());
            if (channel == null) {
                response.onError(Status.NOT_FOUND.asException());
                return;
            }

            final Endpoint inputEndpoint = channel.inputEndpoint.get();
            if (inputEndpoint != null) {
                inputEndpoint.destroy();
            }

            final Endpoint outputEndpoint = channel.outputEndpoint.get();
            if (outputEndpoint != null) {
                outputEndpoint.destroy();
            }

            response.onNext(LCMPS.ChannelDestroyResponse.getDefaultInstance());
            response.onCompleted();
        }

        @Override
        public void status(LCMPS.ChannelStatusRequest request, StreamObserver<LCMPS.ChannelStatus> response) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("Unknown command").asException());
        }

        @Override
        public void statusAll(LCMPS.ChannelStatusAllRequest request, StreamObserver<LCMPS.ChannelStatusList> response) {
            super.statusAll(request, response);
        }

    }

    private class PublicServiceMock extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
        @Override
        public void bind(LCMS.BindRequest request, StreamObserver<LCMS.BindResponse> responseObserver) {
            LOG.info("bind {}", JsonUtils.printRequest(request));
            final String channelName = request.getSlotInstance().getChannelId();
            final SlotInstance slotInstance = ProtoConverter.fromProto(request.getSlotInstance());

            var channel = requireNonNull(directChannels.get(channelName));
            switch (slotInstance.spec().direction()) {
                case INPUT -> {
                    if (!channel.inputSlot.compareAndSet(null, request)) {
                        throw new RuntimeException("INPUT slot already set."
                                                   + " Existing: " + JsonUtils.printSingleLine(channel.inputSlot.get())
                                                   + ", new: " + JsonUtils.printSingleLine(request));
                    }
                    channel.inputEndpoint.set(new Endpoint(slotInstance));
                }
                case OUTPUT -> {
                    if (!channel.outputSlot.compareAndSet(null, request)) {
                        throw new RuntimeException("OUTPUT slot already set."
                                                   + " Existing: " + JsonUtils.printSingleLine(channel.outputSlot.get())
                                                   + ", new: " + JsonUtils.printSingleLine(request));
                    }
                    channel.outputEndpoint.set(new Endpoint(slotInstance));
                }
                default -> throw new RuntimeException("zzz");
            }

            responseObserver.onNext(LCMS.BindResponse.getDefaultInstance());
            responseObserver.onCompleted();

            if (channel.isCompleted()) {
                var inputSlot = requireNonNull(channel.inputSlot.get());
                var outputSlot = requireNonNull(channel.outputSlot.get());

                LOG.info("Connecting channel '" + channelName + "' slots, input='" +
                    inputSlot.getSlotInstance().getSlot().getName() +
                    "', output='" + outputSlot.getSlotInstance().getSlot().getName() + "'...");

                var status = channel.inputEndpoint.get().connect(channel.outputEndpoint.get().slotInstance);

                if (status.getRc().getCode() != LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS) {
                    LOG.error("Failed to connect input slot {} and output slot {}",
                        inputSlot.getSlotInstance().getSlot().getName(),
                        outputSlot.getSlotInstance().getSlot().getName());
                    throw new RuntimeException("slot connect failed: " + JsonUtils.printSingleLine(status));
                }
            }
        }

        @Override
        public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> response) {
            var slotInstance = request.getSlotInstance();
            var channel = directChannels.get(slotInstance.getChannelId());

            if (Objects.isNull(channel)) {
                LOG.error("Can not find direct channel with id {} for slot {}", slotInstance.getChannelId(),
                    slotInstance.getSlot().getName());
                response.onError(Status.NOT_FOUND.withDescription("Slot with unknown channel id").asException());
                return;
            }

            switch (slotInstance.getSlot().getDirection()) {
                case INPUT -> {
                    Endpoint inputEndpoint = channel.inputEndpoint.get();
                    if (inputEndpoint != null) {
                        inputEndpoint.destroy();
                    }
                    response.onNext(LCMS.UnbindResponse.getDefaultInstance());
                }
                case OUTPUT -> {
                    var outputEndpoint = channel.outputEndpoint.get();
                    if (outputEndpoint != null) {
                        outputEndpoint.destroy();
                    }
                    response.onNext(LCMS.UnbindResponse.getDefaultInstance());
                }
                default -> {
                    LOG.error("Invalid direction of the slot {}: {}", slotInstance.getSlot().getName(),
                        slotInstance.getSlot().getDirection());
                    response.onError(Status.INVALID_ARGUMENT.withDescription("Invalid slot direction").asException());
                    return;
                }
            }

            response.onCompleted();
        }

    }
    
}
