package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.portal.config.PortalConfig;
import com.google.common.net.HostAndPort;
import io.micronaut.runtime.Micronaut;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import static ai.lzy.model.UriScheme.LzyFs;

public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

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
        }
        portal.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        portal.awaitTermination();
    }

    @SuppressWarnings("UnstableApiUsage")
    public static void execute(String[] args)
        throws URISyntaxException, IOException
    {
        LOG.info("Executing portal application...");

        var context = Micronaut.run(App.class, args);
        var config = context.getBean(PortalConfig.class);

        // TODO: ssokolvyak -- let's rename 'host' field in config in order to delegate
        //  setting up this property to micronaut
        if (config.getHost() == null) {
            config.setHost(System.getenv(AllocatorAgent.VM_IP_ADDRESS));
        }

        var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsApiPort(), null, null, null);
        var cm = HostAndPort.fromString(config.getChannelManagerAddress());
        var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);

        var allocatorAgent = new AllocatorAgent(config.getToken(), config.getVmId(), config.getAllocatorAddress(),
            config.getAllocatorHeartbeatPeriod(), config.getHost());

        var fsServer = new LzyFsServer(config.getPortalId(), config.getFsRoot(), fsUri, channelManagerUri,
            config.getToken());
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
        throws URISyntaxException, IOException
    {
        execute(args);
    }
}
