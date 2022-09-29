package ai.lzy.tunnel;

import ai.lzy.tunnel.service.LzyTunnelAgentService;
import ai.lzy.util.grpc.ChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TunnelAgentMain {

    private final Server server;

    public TunnelAgentMain(
        LzyTunnelAgentService tunnelAgentService
    ) {
        server = NettyServerBuilder
            .forPort(1234)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(tunnelAgentService)
            .addService(ProtoReflectionService.newInstance())
            .build();
    }

    public void start() throws IOException {
        System.out.println("Starting server");
        server.start();
    }

    public void stop() {
        System.out.println("Stopping server");
        server.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        System.out.println("Awaiting server");
        server.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        TunnelAgentMain tunnelAgent = new TunnelAgentMain(new LzyTunnelAgentService());
        tunnelAgent.start();
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                System.out.println("Stopping server");
                tunnelAgent.stop();
            })
        );
        tunnelAgent.awaitTermination();
    }
}
