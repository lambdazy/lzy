package ai.lzy.portal;


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

    public void stop() {
        portal.close();
    }

    public void awaitTermination() throws InterruptedException {
        portal.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException {
        final var context = ApplicationContext.run();
        final var main = context.getBean(App.class);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping portal service");
            main.stop();
        }));
        main.awaitTermination();
    }
}
