package ai.lzy.graph.test;

import java.nio.file.Path;
import java.util.Map;

public interface GraphExecutorContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
