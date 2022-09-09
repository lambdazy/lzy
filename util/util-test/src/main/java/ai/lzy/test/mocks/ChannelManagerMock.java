package ai.lzy.test.mocks;

import ai.lzy.model.GrpcConverter;
import ai.lzy.model.SlotInstance;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class ChannelManagerMock extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
    private static final Logger LOG = LogManager.getLogger(ChannelManagerMock.class);

    final int port;
    final Server server;

    @SuppressWarnings("UnstableApiUsage")
    public ChannelManagerMock(HostAndPort address) {
        this.port = address.getPort();
        server = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(this))
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

    private static class Endpoint implements Closeable {
        private final LzyFsGrpc.LzyFsBlockingStub fs;
        private final ManagedChannel channel;
        private final SlotInstance slotInstance;

        private Endpoint(SlotInstance slotInstance) {
            this.slotInstance = slotInstance;
            channel = ChannelBuilder.forAddress(slotInstance.uri().getHost() + ":" + slotInstance.uri().getPort())
                .usePlaintext()
                .enableRetry(LzyFsGrpc.SERVICE_NAME)
                .build();
            fs = LzyFsGrpc.newBlockingStub(channel);
        }

        LzyFsApi.SlotCommandStatus connect(SlotInstance to) {
            return fs.connectSlot(LzyFsApi.ConnectSlotRequest.newBuilder()
                .setFrom(GrpcConverter.to(slotInstance))
                .setTo(GrpcConverter.to(to))
                .build()
            );
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        void destroy() {
            fs.destroySlot(LzyFsApi.DestroySlotRequest.newBuilder()
                .setSlotInstance(GrpcConverter.to(slotInstance))
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
        final AtomicReference<ChannelManager.SlotAttach> inputSlot = new AtomicReference<>(null);
        public final AtomicReference<ChannelManager.SlotAttach> outputSlot = new AtomicReference<>(null);

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

    final Map<String, DirectChannelInfo> directChannels = new ConcurrentHashMap<>(); // channelId -> ...

    @Nullable
    public DirectChannelInfo get(String channelId) {
        return directChannels.get(channelId);
    }

    @Override
    public void create(ChannelManager.ChannelCreateRequest request,
                       StreamObserver<ChannelManager.ChannelCreateResponse> response)
    {
        LOG.info("create {}", JsonUtils.printRequest(request));
        if (!request.getChannelSpec().hasDirect()) {
            response.onError(INVALID_ARGUMENT.withDescription("Not direct channel").asException());
            return;
        }

        var channelName = request.getChannelSpec().getChannelName();
        if (directChannels.putIfAbsent(channelName, new DirectChannelInfo()) != null) {
            response.onError(Status.ALREADY_EXISTS.asException());
            return;
        }

        response.onNext(
            ChannelManager.ChannelCreateResponse.newBuilder()
                .setChannelId(channelName)
                .build()
        );
        response.onCompleted();
    }

    @Override
    public void destroy(ChannelManager.ChannelDestroyRequest request,
                        StreamObserver<ChannelManager.ChannelDestroyResponse> response)
    {
        LOG.info("destroy {}", JsonUtils.printRequest(request));
        var channel = directChannels.get(request.getChannelId());
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

        response.onNext(ChannelManager.ChannelDestroyResponse.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void status(ChannelManager.ChannelStatusRequest request,
                       StreamObserver<ChannelManager.ChannelStatus> response)
    {
        response.onError(INVALID_ARGUMENT.withDescription("Unknown command").asException());
    }

    @Override
    public void statusAll(ChannelManager.ChannelStatusAllRequest request,
                          StreamObserver<ChannelManager.ChannelStatusList> responseObserver)
    {
        super.statusAll(request, responseObserver);
    }

    @Override
    public void bind(ChannelManager.SlotAttach request,
                     StreamObserver<ChannelManager.SlotAttachStatus> responseObserver)
    {
        LOG.info("bind {}", JsonUtils.printRequest(request));
        final String channelName = request.getSlotInstance().getChannelId();
        final SlotInstance slotInstance = GrpcConverter.from(request.getSlotInstance());

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

        responseObserver.onNext(ChannelManager.SlotAttachStatus.getDefaultInstance());
        responseObserver.onCompleted();

        if (channel.isCompleted()) {
            var inputSlot = requireNonNull(channel.inputSlot.get());
            var outputSlot = requireNonNull(channel.outputSlot.get());

            System.out.println("Connecting channel '" + channelName + "' slots, input='"
                + inputSlot.getSlotInstance().getSlot().getName()
                + "', output='" + outputSlot.getSlotInstance().getSlot().getName() + "'...");

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
    public void unbind(ChannelManager.SlotDetach request, StreamObserver<ChannelManager.SlotDetachStatus> response) {
        var slotInstance = request.getSlotInstance();
        var channel = directChannels.get(slotInstance.getChannelId());

        if (Objects.isNull(channel)) {
            LOG.error("Can not find direct channel with id {} for slot {}", slotInstance.getChannelId(),
                slotInstance.getSlot().getName());
            response.onError(NOT_FOUND.withDescription("Slot with unknown channel id").asException());
            return;
        }

        switch (slotInstance.getSlot().getDirection()) {
            case INPUT -> {
                Endpoint inputEndpoint = channel.inputEndpoint.get();
                if (inputEndpoint != null) {
                    inputEndpoint.destroy();
                }
                response.onNext(ChannelManager.SlotDetachStatus.getDefaultInstance());
            }
            case OUTPUT -> {
                var outputEndpoint = channel.outputEndpoint.get();
                if (outputEndpoint != null) {
                    outputEndpoint.destroy();
                }
                response.onNext(ChannelManager.SlotDetachStatus.getDefaultInstance());
            }
            default -> {
                LOG.error("Invalid direction of the slot {}: {}", slotInstance.getSlot().getName(),
                    slotInstance.getSlot().getDirection());
                response.onError(INVALID_ARGUMENT.withDescription("Invalid slot direction").asException());
                return;
            }
        }

        response.onCompleted();
    }

    public int port() {
        return port;
    }
}
