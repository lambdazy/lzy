package ai.lzy.kharon.env.client;

import ai.lzy.disk.DiskType;

public interface CachedEnvClient {

    String saveEnvConfig(
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    );

    void markEnvReady(String workflowName, String diskId);

}
