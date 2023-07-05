package ai.lzy.test.context;

import ai.lzy.test.context.config.LzyConfig;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Singleton;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

@Singleton
public class LzyContext {
    private final LzyConfig lzyConfig;
    private final IamContext iamCtx;
    private final AllocatorContext allocatorCtx;
    private final ChannelManagerContext channelManagerCtx;
    private final GraphExecutorContext graphExecutorCtx;
    private final SchedulerContext schedulerCtx;
    private final StorageServiceContext storageCtx;
    private final WhiteboardServiceContext whiteboardCtx;
    private final LzyServiceContext lzyServiceCtx;

    private CountDownLatch running;

    public LzyContext(LzyConfig lzyConfig, IamContext iamCtx, AllocatorContext allocatorCtx,
                      ChannelManagerContext channelManagerCtx, GraphExecutorContext graphExecutorCtx,
                      SchedulerContext schedulerCtx, StorageServiceContext storageCtx,
                      WhiteboardServiceContext whiteboardCtx, LzyServiceContext lzyServiceCtx)
    {

        this.lzyConfig = lzyConfig;
        this.iamCtx = iamCtx;
        this.allocatorCtx = allocatorCtx;
        this.channelManagerCtx = channelManagerCtx;
        this.graphExecutorCtx = graphExecutorCtx;
        this.schedulerCtx = schedulerCtx;
        this.storageCtx = storageCtx;
        this.whiteboardCtx = whiteboardCtx;
        this.lzyServiceCtx = lzyServiceCtx;
    }

    public void setUp() throws Exception {
        running = new CountDownLatch(1);
        setUpIam();
        setUpAllocator();
        setUpChannelManager();
        setUpGraphExecutor();
        setUpScheduler();
        setUpStorageService();
        setUpWhiteboardService();
        setUpLzyService();
    }

    public void tearDown() throws Exception {
        iamCtx.tearDown();
        allocatorCtx.tearDown();
        channelManagerCtx.tearDown();
        graphExecutorCtx.tearDown();
        schedulerCtx.tearDown();
        storageCtx.tearDown();
        whiteboardCtx.tearDown();
        lzyServiceCtx.tearDown();
        running.countDown();
    }

    public void awaitTermination() throws InterruptedException {
        running.await();
    }

    private void setUpLzyService() throws Exception {
        var lzyServicePrefix = "lzy-service.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(lzyServicePrefix + "address", "localhost:" + lzyConfig.getPorts().getLzyServicePort());
        runtimeConfig.put(lzyServicePrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(lzyServicePrefix + "storage.address", "localhost:" + lzyConfig.getPorts().getStoragePort());
        runtimeConfig.put(lzyServicePrefix + "channel-manager-address",
            "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        runtimeConfig.put(lzyServicePrefix + "graph-executor-address",
            "localhost:" + lzyConfig.getPorts().getGraphExecutorPort());
        runtimeConfig.put(lzyServicePrefix + "allocator-address",
            "localhost:" + lzyConfig.getPorts().getAllocatorPort());
        runtimeConfig.put(lzyServicePrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(lzyServicePrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getLzyServiceDbUrl());
        runtimeConfig.put(lzyServicePrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(lzyServicePrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getLzyServiceConfig() != null ?
            Path.of(lzyConfig.getConfigs().getLzyServiceConfig()) : null;

        var envs = lzyConfig.getEnvironments().getLzyServiceEnvironments();

        if (envs != null) {
            lzyServiceCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            lzyServiceCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpWhiteboardService() throws Exception {
        var whiteboardPrefix = "whiteboard.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(whiteboardPrefix + "address", "localhost:" + lzyConfig.getPorts().getWhiteboardPort());
        runtimeConfig.put(whiteboardPrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(whiteboardPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(whiteboardPrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getWhiteboardDbUrl());
        runtimeConfig.put(whiteboardPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(whiteboardPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getWhiteboardConfig() != null ?
            Path.of(lzyConfig.getConfigs().getWhiteboardConfig()) : null;

        var envs = lzyConfig.getEnvironments().getWhiteboardEnvironments();

        if (envs != null) {
            whiteboardCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            whiteboardCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpStorageService() throws Exception {
        var storagePrefix = "storage.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(storagePrefix + "address", "localhost:" + lzyConfig.getPorts().getStoragePort());
        runtimeConfig.put(storagePrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(storagePrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(storagePrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getStorageServiceDbUrl());
        runtimeConfig.put(storagePrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(storagePrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getStorageConfig() != null ?
            Path.of(lzyConfig.getConfigs().getStorageConfig()) : null;

        var envs = lzyConfig.getEnvironments().getStorageEnvironments();

        if (envs != null) {
            storageCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            storageCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpScheduler() throws Exception {
        var schedulerPrefix = "scheduler.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(schedulerPrefix + "port", lzyConfig.getPorts().getSchedulerPort());
        runtimeConfig.put(schedulerPrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(schedulerPrefix + "channel-manager-address",
            "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        runtimeConfig.put(schedulerPrefix + "allocator-address",
            "localhost:" + lzyConfig.getPorts().getAllocatorPort());
        runtimeConfig.put(schedulerPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(schedulerPrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getSchedulerDbUrl());
        runtimeConfig.put(schedulerPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(schedulerPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getSchedulerConfig() != null ?
            Path.of(lzyConfig.getConfigs().getSchedulerConfig()) : null;

        var envs = lzyConfig.getEnvironments().getSchedulerEnvironments();

        if (envs != null) {
            schedulerCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            schedulerCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpGraphExecutor() throws Exception {
        var graphExecutorPrefix = "graph-executor.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(graphExecutorPrefix + "port", lzyConfig.getPorts().getGraphExecutorPort());
        runtimeConfig.put(graphExecutorPrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(graphExecutorPrefix + "scheduler.port", lzyConfig.getPorts().getSchedulerPort());
        runtimeConfig.put(graphExecutorPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(graphExecutorPrefix + LzyConfig.Database.DB_URL,
            lzyConfig.getDatabase().getGraphExecutorDbUrl());
        runtimeConfig.put(graphExecutorPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(graphExecutorPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getGraphExecutorConfig() != null ?
            Path.of(lzyConfig.getConfigs().getGraphExecutorConfig()) : null;

        var envs = lzyConfig.getEnvironments().getGraphExecutorEnvironments();

        if (envs != null) {
            graphExecutorCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            graphExecutorCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpChannelManager() throws Exception {
        var channelManagerPrefix = "channel-manager.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(channelManagerPrefix + "address",
            "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        runtimeConfig.put(channelManagerPrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(channelManagerPrefix + "lzy-service-address",
            "localhost:" + lzyConfig.getPorts().getLzyServicePort());
        runtimeConfig.put(channelManagerPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(channelManagerPrefix + LzyConfig.Database.DB_URL,
            lzyConfig.getDatabase().getChannelManagerDbUrl());
        runtimeConfig.put(channelManagerPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(channelManagerPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getChannelManagerConfig() != null ?
            Path.of(lzyConfig.getConfigs().getChannelManagerConfig()) : null;

        var envs = lzyConfig.getEnvironments().getChannelManagerEnvironments();

        if (envs != null) {
            channelManagerCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            channelManagerCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpAllocator() throws Exception {
        var allocatorPrefix = "allocator.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(allocatorPrefix + "port", lzyConfig.getPorts().getAllocatorPort());
        runtimeConfig.put(allocatorPrefix + "iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        runtimeConfig.put("allocator.thread-allocator.enabled", true);
        runtimeConfig.put("allocator.thread-allocator.vm-class-name", "ai.lzy.worker.Worker");
        runtimeConfig.put(allocatorPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(allocatorPrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getAllocatorDbUrl());
        runtimeConfig.put(allocatorPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(allocatorPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getAllocatorConfig() != null ?
            Path.of(lzyConfig.getConfigs().getAllocatorConfig()) : null;

        var envs = lzyConfig.getEnvironments().getAllocatorEnvironments();

        if (envs != null) {
            allocatorCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            allocatorCtx.setUp(config, runtimeConfig);
        }
    }

    private void setUpIam() throws Exception {
        var iamPrefix = "iam.";
        var runtimeConfig = new HashMap<String, Object>();
        runtimeConfig.put(iamPrefix + "serverPort", lzyConfig.getPorts().getIamPort());
        runtimeConfig.put(iamPrefix + LzyConfig.Database.DB_ENABLED, true);
        runtimeConfig.put(iamPrefix + LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getIamDbUrl());
        runtimeConfig.put(iamPrefix + LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        runtimeConfig.put(iamPrefix + LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        var config = lzyConfig.getConfigs().getIamConfig() != null ?
            Path.of(lzyConfig.getConfigs().getIamConfig()) : null;

        var envs = lzyConfig.getEnvironments().getIamEnvironments();

        if (envs != null) {
            iamCtx.setUp(config, runtimeConfig, toArray(envs));
        } else {
            iamCtx.setUp(config, runtimeConfig);
        }
    }

    public static void main(String[] args) throws Exception {
        try (var micronautCtx = Micronaut.run(args)) {
            var lzy = micronautCtx.getBean(LzyContext.class);

            Runtime.getRuntime().addShutdownHook(new Thread(lzy.running::countDown));

            try {
                lzy.setUp();
                lzy.awaitTermination();
            } catch (InterruptedException e) {
                // intentionally blank
            } finally {
                lzy.tearDown();
            }
        }
    }

    private static String[] toArray(Collection<String> collection) {
        return collection.toArray(new String[0]);
    }
}
