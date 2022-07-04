package ai.lzy.kharon.env.manager;

import ai.lzy.disk.DiskType;
import ai.lzy.kharon.env.CachedEnv;

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
