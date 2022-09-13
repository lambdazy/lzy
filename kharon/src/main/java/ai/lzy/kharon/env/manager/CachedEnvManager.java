package ai.lzy.kharon.env.manager;

import ai.lzy.disk.model.DiskType;
import ai.lzy.kharon.env.CachedEnv;

public interface CachedEnvManager {

    CachedEnv saveEnvConfig(
        String userId,
        String workflowName,
        String dockerImage,
        String condaYaml,
        DiskType diskType
    );

    void markEnvReady(
        String userId,
        String workflowName,
        String diskId
    );

}
