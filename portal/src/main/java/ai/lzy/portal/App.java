package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.portal.config.PortalConfig;
import com.google.common.net.HostAndPort;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static ai.lzy.model.UriScheme.LzyFs;

@Singleton
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    private final Portal portal;

    public App(Portal portal) {
        this.portal = portal;
    }

    public void start() {
        portal.start();
    }

    public void stop() {
        portal.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        portal.awaitTermination();
    }

    @SuppressWarnings("UnstableApiUsage")
    public static void main(String[] args)
        throws InterruptedException, AllocatorAgent.RegisterException, URISyntaxException, IOException {

        var context = ApplicationContext.run();
        var config = context.getBean(PortalConfig.class);

        if (Objects.isNull(config.getHost())) {
            config.setHost(System.getenv(AllocatorAgent.VM_IP_ADDRESS));
        }

        var fsUri = new URI(LzyFs.scheme(), null, config.getHost(), config.getFsApiPort(), null, null, null);
        var cm = HostAndPort.fromString(config.getChannelManagerAddress());
        var channelManagerUri = new URI("http", null, cm.getHost(), cm.getPort(), null, null, null);

        var allocatorAgent = new AllocatorAgent(config.getToken(), null, null, null, config.getHost());
        var fsServer = new LzyFsServer(config.getPortalId(), config.getFsRoot(), fsUri, channelManagerUri,
            config.getToken());
        var main = new App(new Portal(config, allocatorAgent, fsServer));

        main.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop();
        }));
        main.awaitTermination();
    }
}
