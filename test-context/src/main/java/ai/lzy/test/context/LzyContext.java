package ai.lzy.test.context;

import ai.lzy.test.context.config.LzyConfig;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Singleton
public class LzyContext {
    private final ApplicationContext micronautCtx;
    private final LzyConfig lzyConfig;
    private final IamContext iamCtx;
    private final AllocatorContext allocatorCtx;
    private final ChannelManagerContext channelManagerCtx;
    private final GraphExecutorContext graphExecutorCtx;
    private final SchedulerContext schedulerCtx;
    private final WhiteboardServiceContext whiteboardCtx;
    private final LzyServiceContext lzyServiceCtx;

    private CountDownLatch running;

    public LzyContext(ApplicationContext micronautCtx, LzyConfig lzyConfig, IamContext iamCtx,
                      AllocatorContext allocatorCtx, ChannelManagerContext channelManagerCtx,
                      GraphExecutorContext graphExecutorCtx, SchedulerContext schedulerCtx,
                      WhiteboardServiceContext whiteboardCtx, LzyServiceContext lzyServiceCtx)
    {
        this.micronautCtx = micronautCtx;
        this.lzyConfig = lzyConfig;
        this.iamCtx = iamCtx;
        this.allocatorCtx = allocatorCtx;
        this.channelManagerCtx = channelManagerCtx;
        this.graphExecutorCtx = graphExecutorCtx;
        this.schedulerCtx = schedulerCtx;
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
        setUpWhiteboardService();
        setUpLzyService();
    }

    public void tearDown() throws Exception {
        iamCtx.tearDown();
        allocatorCtx.tearDown();
        channelManagerCtx.tearDown();
        graphExecutorCtx.tearDown();
        schedulerCtx.tearDown();
        whiteboardCtx.tearDown();
        lzyServiceCtx.tearDown();
        running.countDown();
    }

    public void awaitTermination() throws InterruptedException {
        running.await();
    }

    private void setUpLzyService() throws Exception {
        var lzyServicePrefix = "lzy-service";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(lzyServicePrefix));

        configOverrides.put("address", "localhost:" + lzyConfig.getPorts().getLzyServicePort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put("channel-manager-address", "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        configOverrides.put("graph-executor-address", "localhost:" + lzyConfig.getPorts().getGraphExecutorPort());
        configOverrides.put("allocator-address", "localhost:" + lzyConfig.getPorts().getAllocatorPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getLzyServiceDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> lzyServicePrefix + "." + e.getKey(), Map.Entry::getValue));

        var configPath = lzyConfig.getConfigs().getLzyServiceConfig();
        var envs = lzyConfig.getEnvironments().getLzyServiceEnvironments();

        lzyServiceCtx.setUp(configPath, configOverrides, toArray(envs));
    }

    private void setUpWhiteboardService() throws Exception {
        var whiteboardPrefix = "whiteboard";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(whiteboardPrefix));

        configOverrides.put("address", "localhost:" + lzyConfig.getPorts().getWhiteboardPort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getWhiteboardDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> whiteboardPrefix + "." + e.getKey(), Map.Entry::getValue));

        var configPath = lzyConfig.getConfigs().getWhiteboardConfig();
        var envs = lzyConfig.getEnvironments().getWhiteboardEnvironments();

        whiteboardCtx.setUp(configPath, configOverrides, toArray(envs));
    }

    private void setUpScheduler() throws Exception {
        var schedulerPrefix = "scheduler";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(schedulerPrefix));

        configOverrides.put("port", lzyConfig.getPorts().getSchedulerPort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put("channel-manager-address", "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        configOverrides.put("allocator-address", "localhost:" + lzyConfig.getPorts().getAllocatorPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getSchedulerDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> schedulerPrefix + "." + e.getKey(), Map.Entry::getValue));

        var configPath = lzyConfig.getConfigs().getSchedulerConfig();
        var envs = lzyConfig.getEnvironments().getSchedulerEnvironments();

        schedulerCtx.setUp(configPath, configOverrides, toArray(envs));
    }

    private void setUpGraphExecutor() throws Exception {
        var graphExecutorPrefix = "graph-executor";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(graphExecutorPrefix));

        configOverrides.put("port", lzyConfig.getPorts().getGraphExecutorPort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put("scheduler.port", lzyConfig.getPorts().getSchedulerPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getGraphExecutorDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> graphExecutorPrefix + "." + e.getKey(), Map.Entry::getValue));

        var configPath = lzyConfig.getConfigs().getGraphExecutorConfig();
        var envs = lzyConfig.getEnvironments().getGraphExecutorEnvironments();

        graphExecutorCtx.setUp(configPath, configOverrides, toArray(envs));
    }

    private void setUpChannelManager() throws Exception {
        var channelManagerPrefix = "channel-manager";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(channelManagerPrefix));

        configOverrides.put("address", "localhost:" + lzyConfig.getPorts().getChannelManagerPort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put("lzy-service-address", "localhost:" + lzyConfig.getPorts().getLzyServicePort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getChannelManagerDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> channelManagerPrefix + "." + e.getKey(), Map.Entry::getValue));

        var config = lzyConfig.getConfigs().getChannelManagerConfig();
        var envs = lzyConfig.getEnvironments().getChannelManagerEnvironments();

        channelManagerCtx.setUp(config, configOverrides, toArray(envs));
    }

    private void setUpAllocator() throws Exception {
        var allocatorPrefix = "allocator";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(allocatorPrefix));

        configOverrides.put("port", lzyConfig.getPorts().getAllocatorPort());
        configOverrides.put("iam.address", "localhost:" + lzyConfig.getPorts().getIamPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getAllocatorDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> allocatorPrefix + "." + e.getKey(), Map.Entry::getValue));

        var config = lzyConfig.getConfigs().getAllocatorConfig();
        var envs = lzyConfig.getEnvironments().getAllocatorEnvironments();

        allocatorCtx.setUp(config, configOverrides, toArray(envs));
    }

    private void setUpIam() throws Exception {
        var iamPrefix = "iam";

        Map<String, Object> configOverrides = new HashMap<>(micronautCtx.getProperties(iamPrefix));

        configOverrides.put("serverPort", lzyConfig.getPorts().getIamPort());
        configOverrides.put(LzyConfig.Database.DB_ENABLED, true);
        configOverrides.put(LzyConfig.Database.DB_URL, lzyConfig.getDatabase().getIamDbUrl());
        configOverrides.put(LzyConfig.Database.DB_USERNAME, LzyConfig.Database.POSTGRES_USERNAME);
        configOverrides.put(LzyConfig.Database.DB_PASSWORD, LzyConfig.Database.POSTGRES_PASSWORD);

        configOverrides.values().removeIf(Objects::isNull);
        configOverrides = configOverrides.entrySet().stream().collect(
            Collectors.toMap(e -> iamPrefix + "." + e.getKey(), Map.Entry::getValue));

        var configPath = lzyConfig.getConfigs().getIamConfig();
        var envs = lzyConfig.getEnvironments().getIamEnvironments();

        iamCtx.setUp(configPath, configOverrides, toArray(envs));
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

    private static String[] toArray(@Nullable Collection<String> collection) {
        var empty = new String[0];
        return Objects.nonNull(collection) ? collection.toArray(empty) : empty;
    }
}
