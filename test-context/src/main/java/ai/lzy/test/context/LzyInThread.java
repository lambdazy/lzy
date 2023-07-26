package ai.lzy.test.context;

import ai.lzy.test.context.config.LzyConfig;
import ai.lzy.test.context.config.Utils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class LzyInThread {
    private Thread thread;
    private ApplicationContext micronautCtx;

    public void setUp(LzyConfig.Configs configs, LzyConfig.Environments servicesCtxEnvs, LzyConfig.Ports ports,
                      LzyConfig.Database database, String... environments) throws InterruptedException
    {
        setUp(configs, Collections.emptyMap(), servicesCtxEnvs, ports, database, environments);
    }

    public void setUp(LzyConfig.Configs configs, Map<String, Object> configOverrides,
                      LzyConfig.Environments servicesCtxEnvs, LzyConfig.Ports ports,
                      LzyConfig.Database database, String... environments)
        throws InterruptedException
    {
        var args = new ArrayList<String>() {
            {
                addAll(configs.asCmdArgs());
                addAll(Utils.asCmdArgs(configOverrides));
                addAll(servicesCtxEnvs.asCmdArgs());
                addAll(ports.asCmdArgs());
                addAll(database.asCmdArg());
            }
        };
        micronautCtx = Micronaut.build(args.toArray(new String[0]))
            .environments(environments)
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
