package ai.lzy.kharon.env;

import ai.lzy.disk.Disk;
import ai.lzy.kharon.env.dao.CachedEnvDao;

public record CachedEnv(
    String envId,
    String workflowName,
    Disk disk,
    CachedEnvStatus status,
    String dockerImage,
    String yamlConfig,
    String dockerPullPolicy // enum: always, ifAbsent
) {

    public CachedEnv(CachedEnvDao.CachedEnvInfo envInfo, Disk disk) {
        this(envInfo.envId(), envInfo.workflowName(), disk, envInfo.status(),
            envInfo.dockerImage(), envInfo.yamlConfig(), "TODO");
    }
}
