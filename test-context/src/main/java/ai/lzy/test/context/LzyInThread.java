package ai.lzy.test.context;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LzyInThread {
    private Thread thread;
    private ApplicationContext micronautCtx;

    public void setUp(List<String> args, List<String> environments) throws InterruptedException {
        micronautCtx = Micronaut.build(args.toArray(new String[0]))
            .environments(environments.toArray(new String[0]))
            .mainClass(LzyContext.class)
            .start();

        var setupFlag = new CountDownLatch(1);

        thread = new Thread(() -> {
            try {
                var lzy = micronautCtx.getBean(LzyContext.class);

                try {
                    lzy.setUp();
                    setupFlag.countDown();
                    lzy.awaitTermination();
                } catch (InterruptedException e) {
                    // intentionally blank
                } finally {
                    setupFlag.countDown();
                    lzy.tearDown();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread.setName("lzy-context-runner");
        thread.setDaemon(true);
        thread.start();
        setupFlag.await();
    }

    public void tearDown() {
        thread.interrupt();
        try {
            thread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            micronautCtx.close();
        }
    }

    public ApplicationContext micronautContext() {
        return micronautCtx;
    }
}
