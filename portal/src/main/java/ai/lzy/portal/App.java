package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.util.auth.credentials.JwtUtils;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Requires;
import io.micronaut.runtime.Micronaut;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

import static ai.lzy.model.UriScheme.LzyFs;

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

    public static void execute(String[] args)
        throws URISyntaxException, IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
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

        var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsApiPort(), null, null, null);
        var cm = HostAndPort.fromString(config.getChannelManagerAddress());
        var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);
        var portalAddress = "%s:%s".formatted(config.getHost(), config.getPortalApiPort());

        var allocatorAgent = new AllocatorAgent(config.getAllocatorToken(),
            config.getVmId(), config.getAllocatorAddress(), config.getAllocatorHeartbeatPeriod(), portalAddress);

        var fsServerJwt = JwtUtils.buildJWT(config.getPortalId(), "INTERNAL", Date.from(Instant.now()),
            JwtUtils.afterDays(7), new StringReader(config.getIamPrivateKey()));
        var fsServer = new LzyFsServer(config.getPortalId(), config.getFsRoot(), fsUri,
            channelManagerUri, fsServerJwt, false);

        var main = new App(new Portal(config, allocatorAgent, fsServer));
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

    public static void main(String[] args)
        throws URISyntaxException, IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        execute(args);
    }
}
