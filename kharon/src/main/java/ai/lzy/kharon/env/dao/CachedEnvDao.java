package ai.lzy.kharon.env.dao;

import ai.lzy.kharon.env.CachedEnvStatus;

import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface CachedEnvDao {

    void insertEnv(CachedEnvInfo cachedEnv);

    CachedEnvInfo setEnvStatus(String envId, CachedEnvStatus status);

    @Nullable
    CachedEnvInfo findEnv(String envId);

    @Nullable
    CachedEnvInfo findEnv(String userId, String workflowName, String diskId);

    Stream<CachedEnvInfo> listEnvs(String userId, String workflowName);

    void deleteEnv(String envId);

    void deleteWorkflowEnvs(String userId, String workflowName);

    record CachedEnvInfo(
        String envId,
        String userId,
        String workflowName,
        String diskId,
        CachedEnvStatus status,
        String dockerImage,
        String yamlConfig
    ) {}

}
