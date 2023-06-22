package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.model.Constants;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.services.PortalSlotsService;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.Objects;

@Singleton
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final ApplicationContext context;
    private final PortalConfig config;

    private final Server mainServer;
    private final Server slotsServer;

    private final PortalSlotsService slotsService;
    private final AllocatorAgent allocatorAgent;

    public App(ApplicationContext context, PortalConfig config,
               @Named("PortalGrpcServer") Server mainServer,
               @Named("PortalSlotsGrpcServer") Server slotsServer,
               @Named("PortalSlotsService") PortalSlotsService slotsService,
               @Named("PortalAllocatorAgent") AllocatorAgent allocatorAgent)
    {
        this.context = context;
        this.config = config;
        this.mainServer = mainServer;
        this.slotsServer = slotsServer;
        this.slotsService = slotsService;
        this.allocatorAgent = allocatorAgent;
    }

    public void start() throws AllocatorAgent.RegisterException, IOException {
        LOG.info("Executing portal application with config: {}", config.toSafeString());

        slotsService.start();
        allocatorAgent.start(Map.of(
            Constants.PORTAL_ADDRESS_KEY, HostAndPort.fromParts(config.getHost(), config.getPortalApiPort()).toString(),
            Constants.FS_ADDRESS_KEY, HostAndPort.fromParts(config.getHost(), config.getSlotsApiPort()).toString()
        ));
        slotsServer.start();

        mainServer.start();
    }

    @PreDestroy
    public void stop() {
        LOG.info("Stop portal main application: {}", config.toSafeString());
        allocatorAgent.shutdown();
        mainServer.shutdownNow();
        context.stop();
    }

    public void awaitTermination() throws InterruptedException {
        mainServer.awaitTermination();
    }

    public static void execute(String[] args)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, AllocatorAgent.RegisterException
    {
        var context = Micronaut.build(args)
            .mainClass(App.class)
            // XXX: tests workaround
            .properties(Map.of("micronaut.server.port", "-1"))
            .start();

        var config = context.getBean(PortalConfig.class);

        if (config.getHost() == null) {
            config.setHost(System.getenv(AllocatorAgent.VM_IP_ADDRESS));
        }
        if (config.getAllocatorToken() == null) {
            config.setAllocatorToken(System.getenv(AllocatorAgent.VM_ALLOCATOR_OTT));
        }
        if (config.getIamPrivateKey() == null) {
            config.setIamPrivateKey(System.getenv(ENV_PORTAL_PKEY));
        }

        Objects.requireNonNull(config.getAllocatorToken());
        Objects.requireNonNull(config.getIamPrivateKey());

        var main = context.getBean(App.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop();
        }));

        main.start();

        try {
            main.awaitTermination();
        } catch (InterruptedException e) {
            LOG.warn("Portal main thread was interrupted");
        }

        main.stop();
    }

    public static void main(String[] args)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, AllocatorAgent.RegisterException
    {
        execute(args);
    }
}
