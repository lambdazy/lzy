package ai.lzy.whiteboard.test;

import ai.lzy.test.context.WhiteboardServiceContext;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.whiteboard.test.WhiteboardServiceContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class WhiteboardServiceContextImpl implements WhiteboardServiceContext {
    public static final String ENV_NAME = "common_whiteboard_context";

    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception {

    }

    @Override
    public void tearDown() {

    }
}
