package ai.lzy.tunnel;

import ai.lzy.tunnel.config.ServiceConfig;
import ai.lzy.tunnel.service.LzyTunnelAgentService;
import ai.lzy.util.grpc.GrpcExceptionHandlingInterceptor;
import ai.lzy.util.grpc.GrpcUtils;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class TunnelAgentMain {
    private static final Logger LOG = LogManager.getLogger(TunnelAgentMain.class);

    private final Server server;
    private final HostAndPort address;

    public TunnelAgentMain(LzyTunnelAgentService tunnelAgentService, String addr) {
        this.address = HostAndPort.fromString(addr);
        this.server = GrpcUtils.newGrpcServer(address, GrpcUtils.NO_AUTH)
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
