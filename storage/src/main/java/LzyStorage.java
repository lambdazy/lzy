import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class LzyStorage {
    private static final Logger LOG = LogManager.getLogger(LzyStorage.class);

    public LzyStorage(ApplicationContext context) {

    }

    public void start() throws IOException {
        //server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            //iamServer.shutdown();
        }));
    }

    public void close() {
        //server.shutdownNow();
    }

    public void awaitTermination() throws InterruptedException {
        //server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            var app = new LzyStorage(context);

            app.start();
            app.awaitTermination();
        } catch (NoSuchBeanException e) {
            LOG.fatal(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
