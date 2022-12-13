package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.model.Constants;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.services.PortalSlotsService;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.runtime.Micronaut;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.Objects;

@Requires
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final ApplicationContext context;

    public App(ApplicationContext context) {
        this.context = context;
    }

    public void start() throws AllocatorAgent.RegisterException, IOException {
        var config = context.getBean(PortalConfig.class);
        LOG.info("Executing portal application with config: {}", config.toSafeString());

        var slotsService = context.getBean(PortalSlotsService.class, Qualifiers.byName("PortalSlotsService"));
        var allocatorAgent = context.getBean(AllocatorAgent.class, Qualifiers.byName("PortalAllocatorAgent"));
        var portalServer = context.getBean(Server.class, Qualifiers.byName("PortalGrpcServer"));
        var slotsServer = context.getBean(Server.class, Qualifiers.byName("PortalSlotsGrpcServer"));

        slotsService.start();
        allocatorAgent.start(Map.of(
            Constants.PORTAL_ADDRESS_KEY, HostAndPort.fromParts(config.getHost(), config.getPortalApiPort()).toString(),
            Constants.FS_ADDRESS_KEY, HostAndPort.fromParts(config.getHost(), config.getSlotsApiPort()).toString()
        ));
        slotsServer.start();
        portalServer.start();
    }

    public void stop() {
        context.stop();
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

        var main = new App(context);
        main.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop();
        }));
    }

    public static void main(String[] args)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, AllocatorAgent.RegisterException
    {
        execute(args);
    }
}
