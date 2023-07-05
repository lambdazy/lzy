package ai.lzy.test.context;

import java.nio.file.Path;
import java.util.Map;

public interface ServiceContext {
    String IAM_BEAN_NAME = "IamServiceContext";
    String ALLOCATOR_BEAN_NAME = "AllocatorServiceContext";
    String CHANNEL_MANAGER_BEAN_NAME = "ChannelManagerServiceContext";
    String GRAPH_EXECUTOR_BEAN_NAME = "GraphExecutorServiceContext";
    String SCHEDULER_BEAN_NAME = "SchedulerServiceContext";
    String LZY_SERVICE_BEAN_NAME = "LzyServiceContext";
    String STORAGE_BEAN_NAME = "StorageServiceContext";
    String WHITEBOARD_BEAN_NAME = "WhiteboardServiceContext";

    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
