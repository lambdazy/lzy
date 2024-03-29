package ai.lzy.scheduler.test;

import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.test.context.SchedulerContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.scheduler.test.SchedulerContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class SchedulerContextImpl implements SchedulerContext {
    public static final String ENV_NAME = "common_scheduler_context";

    private ApplicationContext micronautContext;
    private SchedulerApi schedulerApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("scheduler", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        schedulerApp = micronautContext.getBean(SchedulerApi.class);
    }

    @Override
    public void tearDown() {
        schedulerApp.close();
        try {
            schedulerApp.awaitTermination();
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            micronautContext.stop();
        }
    }
}
