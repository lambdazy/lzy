package ai.lzy.env.dao;

import ai.lzy.disk.DiskType;
import ai.lzy.env.CachedEnvStatus;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface CachedEnvDao {

    void insertEnv(CachedEnvInfo cachedEnv);

    CachedEnvInfo setEnvStatus(String envId, CachedEnvStatus status);

    @Nullable CachedEnvInfo findEnv(String envId);

    @Nullable CachedEnvInfo findEnv(String workflowName, String diskId);

    Stream<CachedEnvInfo> listEnvs(String workflowName);

    Stream<CachedEnvInfo> listEnvs(String workflowName, DiskType diskType);

    void deleteEnv(String envId);

    void deleteWorkflowEnvs(String workflowName);

    record CachedEnvInfo(
        String envId,
        String workflowName,
        String diskId,
        CachedEnvStatus status,
        String dockerImage,
        String yamlConfig
    ) {}

}
