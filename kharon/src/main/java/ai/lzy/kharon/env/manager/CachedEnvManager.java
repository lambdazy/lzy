package ai.lzy.kharon.env.manager;

import ai.lzy.kharon.env.CachedEnv;
import ai.lzy.kharon.env.model.DiskType;

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
