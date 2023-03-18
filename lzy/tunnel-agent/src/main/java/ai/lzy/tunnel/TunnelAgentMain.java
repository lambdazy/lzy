package ai.lzy.tunnel;

import ai.lzy.tunnel.config.ServiceConfig;
import ai.lzy.tunnel.service.LzyTunnelAgentService;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcExceptionHandlingInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TunnelAgentMain {
    private static final Logger LOG = LogManager.getLogger(TunnelAgentMain.class);

    private final Server server;
    private final HostAndPort address;

    public TunnelAgentMain(LzyTunnelAgentService tunnelAgentService, String addr) {
        this.address = HostAndPort.fromString(addr);
        this.server = NettyServerBuilder
            .forPort(address.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(GrpcExceptionHandlingInterceptor.server())
            .addService(tunnelAgentService)
            .addService(ProtoReflectionService.newInstance())
            .build();
    }

    public void start() throws IOException {
        LOG.info("Starting server on {}", address);
        server.start();
    }

    public void stop() {
        LOG.info("Stopping server");
        server.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        LOG.info("Awaiting server");
        server.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (ApplicationContext context = ApplicationContext.run()) {
            try {
                ServiceConfig config = context.getBean(ServiceConfig.class);
                var tunnelAgent = new TunnelAgentMain(
                    context.getBean(LzyTunnelAgentService.class),
                    config.getAddress()
                );

                tunnelAgent.start();
                Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> {
                        LOG.info("Stopping server");
                        tunnelAgent.stop();
                    })
                );
                tunnelAgent.awaitTermination();
            } catch (NoSuchBeanException e) {
                LOG.error("Shutdown, ", e);
                System.exit(-1);
            }
        }
    }
}
