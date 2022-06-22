package ru.yandex.cloud.ml.platform.lzy.fs.mock;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ServiceMock<T extends BindableService> {
    public static final String HOSTNAME = "localhost";
    private final T instance;
    private final int port;
    private Server server;
    private final List<ManagedChannel> channels = new ArrayList<>();

    public ServiceMock(T instance) {
        this.instance = instance;
        port = FreePortFinder.find(10000, 20000);
    }

    public int port() {
        return port;
    }
    public Channel channel() {
        ManagedChannel channel = ChannelBuilder
            .forAddress(HOSTNAME, port)
            .usePlaintext()
            .build();
        channels.add(channel);
        return channel;
    }

    public void start() throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(HOSTNAME, port))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(instance)
            .build();
        server.start();
    }
    public void stop() {
        channels.forEach(ManagedChannel::shutdown);
        server.shutdown();
    }
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }
}
