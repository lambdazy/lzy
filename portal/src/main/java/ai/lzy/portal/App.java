package ai.lzy.portal;

import ai.lzy.portal.config.PortalConfig;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public static void main(String[] args) throws InterruptedException {
        var context = ApplicationContext.run();
        var config = context.getBean(PortalConfig.class);

        var main = new App(new Portal(config));
        main.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop();
        }));

        main.awaitTermination();
    }
}
