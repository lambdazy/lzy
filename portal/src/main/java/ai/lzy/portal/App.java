package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.portal.config.PortalConfig;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.Micronaut;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Objects;

@Requires
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static final String ENV_PORTAL_PKEY = "LZY_PORTAL_PKEY";

    private final Portal portal;

    public App(Portal portal) {
        this.portal = portal;
    }

    public void start() {
        portal.start();
    }

    public void stop(boolean force) {
        if (force) {
            portal.shutdownNow();
        } else {
            portal.shutdown();
        }
    }

    public void awaitTermination() throws InterruptedException {
        portal.awaitTermination();
    }

    public static void execute(String[] args) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        LOG.info("Executing portal application...");

        var context = Micronaut.run(App.class, args);
        var config = context.getBean(PortalConfig.class);

        // TODO: ssokolvyak -- let's rename 'host' field in config in order to delegate
        //  setting up this property to micronaut
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

        var portalAddress = "%s:%d".formatted(config.getHost(), config.getPortalApiPort());

        var allocatorAgent = new AllocatorAgent(config.getAllocatorToken(),
            config.getVmId(), config.getAllocatorAddress(), config.getAllocatorHeartbeatPeriod());

        var main = new App(new Portal(config, allocatorAgent, null));
        main.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop(false);
        }));

        try {
            main.awaitTermination();
        } catch (InterruptedException e) {
            LOG.debug("Was interrupted while waiting for portal termination");
            main.stop(true);
        }
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        execute(args);
    }
}
