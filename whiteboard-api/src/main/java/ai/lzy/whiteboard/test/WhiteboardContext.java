package ai.lzy.whiteboard.test;

import java.nio.file.Path;
import java.util.Map;

public interface WhiteboardContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
