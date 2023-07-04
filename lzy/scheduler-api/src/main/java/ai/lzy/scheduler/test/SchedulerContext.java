package ai.lzy.scheduler.test;

import java.nio.file.Path;
import java.util.Map;

public interface SchedulerContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String...environments) throws Exception;

    void tearDown();
}
