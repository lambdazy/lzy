package ai.lzy.env.manager;

import ai.lzy.disk.DiskType;
import ai.lzy.env.CachedEnv;

public interface CachedEnvManager {

    CachedEnv saveEnvConfig(
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    );

    void markEnvReady(
        String workflowName,
        String diskId
    );

}
