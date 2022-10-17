package ai.lzy.tunnel;

import ai.lzy.tunnel.config.TunnelAgentConfig;
import ai.lzy.tunnel.service.LzyTunnelAgentService;
import ai.lzy.util.grpc.ChannelBuilder;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Singleton
public class TunnelAgentMain {
    private static final Logger LOG = LogManager.getLogger(TunnelAgentMain.class);

    private final Server server;
    private final TunnelAgentConfig config;

    @Inject
    public TunnelAgentMain(
        LzyTunnelAgentService tunnelAgentService,
        TunnelAgentConfig config
    ) {
        HostAndPort address = HostAndPort.fromString(config.address());
        this.server = NettyServerBuilder
            .forPort(address.getPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(tunnelAgentService)
            .addService(ProtoReflectionService.newInstance())
            .build();
        this.config = config;
    }

    public void start() throws IOException {
        LOG.info("Starting server on {}", config.address());
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
        final TunnelAgentMain tunnelAgent;
        try (ApplicationContext context = ApplicationContext.run()) {
            try {
                tunnelAgent = context.getBean(TunnelAgentMain.class);

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
