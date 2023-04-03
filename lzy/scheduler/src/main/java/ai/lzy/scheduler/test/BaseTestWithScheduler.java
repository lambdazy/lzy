package ai.lzy.scheduler.test;

import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.test.GrpcUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class BaseTestWithScheduler {
    private ApplicationContext context;
    private SchedulerApi scheduler;

    private int port;

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var schedulerConfig = new YamlPropertySourceLoader()
            .read("scheduler", new FileInputStream("../scheduler/src/main/resources/application-test.yml"));
        schedulerConfig.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(schedulerConfig));
        var config = context.getBean(ServiceConfig.class);
        config.setPort(GrpcUtils.rollPort());
        port = config.getPort();
        config.setSchedulerAddress("localhost:" + port);
        this.scheduler = context.getBean(SchedulerApi.class);
    }

    public void after() throws InterruptedException {
        scheduler.close();
        scheduler.awaitTermination();
        context.stop();
    }

    public ApplicationContext getContext() {
        return context;
    }

    public int getPort() {
        return port;
    }
}
