package ai.lzy.kharon.env.manager;

import ai.lzy.disk.Disk;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.dao.DiskDao;
import ai.lzy.disk.manager.DiskManager;
import ai.lzy.kharon.env.CachedEnv;
import ai.lzy.kharon.env.CachedEnvStatus;
import ai.lzy.kharon.env.dao.CachedEnvDao;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(beans = DiskDao.class)
@Requires(beans = CachedEnvDao.class)
public class CachedEnvManagerImpl implements CachedEnvManager {

    private static final Logger LOG = LogManager.getLogger(CachedEnvManagerImpl.class);

    @Inject
    private CachedEnvDao cachedEnvDao;
    @Inject
    private DiskManager diskManager;

    @Override
    public CachedEnv saveEnvConfig(
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    ) {
        CachedEnv existedEnv = findConfigRelatedEnv(workflowName, dockerImage, yamlConfig, diskType);
        if (existedEnv != null) {
            return existedEnv;
        }

        clearWorkflowEnvs(workflowName);

        CachedEnv newEnv = createCachedEnv(workflowName, dockerImage, yamlConfig, diskType);

        return newEnv;
    }

    @Override
    public void markEnvReady(String workflowName, String diskId) {
        final var envInfo = cachedEnvDao.findEnv(workflowName, diskId);
        if (envInfo == null) {
            String errorMessage = String.format(
                "Failed to mark env ready, env (workflowName=%s, diskId=%s) not found",
                workflowName, diskId
            );
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        final var updatedEnvInfo = cachedEnvDao.setEnvStatus(envInfo.envId(), CachedEnvStatus.READY);
    }

    private @Nullable CachedEnv findConfigRelatedEnv(
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    ) {
        final List<CachedEnvDao.CachedEnvInfo> configRelatedEnvs = cachedEnvDao.listEnvs(workflowName,
                diskType)
            .filter(env -> env.dockerImage().equals(dockerImage))
            .filter(env -> env.yamlConfig().equals(yamlConfig))
            .toList();

        if (configRelatedEnvs.isEmpty()) {
            return null;
        }

        final CachedEnvDao.CachedEnvInfo envInfo = configRelatedEnvs.get(0);
        if (configRelatedEnvs.size() > 1) {
            LOG.warn(
                "Unexpected multiple cached envs with similar config (envIds: {}). Used env with envId={}",
                String.join(", ", configRelatedEnvs.stream().map(it -> it.envId()).toList()),
                envInfo.envId()
            );
        }

        Disk disk = diskManager.findDisk(envInfo.diskId());
        if (disk == null) {
            return null;
        }

        return new CachedEnv(envInfo, disk);
    }

    private void clearWorkflowEnvs(String workflowName) {
        final List<CachedEnvDao.CachedEnvInfo> workflowEnvs = cachedEnvDao.listEnvs(workflowName).toList();
        cachedEnvDao.deleteWorkflowEnvs(workflowName);

        final List<String> aliveDisks = new ArrayList<>();
        AtomicReference<RuntimeException> failedDiskDeletion = new AtomicReference<>();
        workflowEnvs.forEach(env -> {
            try {
                diskManager.deleteDisk(env.diskId());
            } catch (RuntimeException e) {
                aliveDisks.add(env.diskId());
                failedDiskDeletion.set(e);
            }
        });
        if (!aliveDisks.isEmpty()) {
            LOG.error("Failed to delete disks {}", String.join(",", aliveDisks));
            throw failedDiskDeletion.get();
        }
    }

    private CachedEnv createCachedEnv(
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    ) {
        String envId = "env-" + UUID.randomUUID();
        Disk disk = diskManager.createDisk("env-disk", diskType, 0);
        final CachedEnvDao.CachedEnvInfo envInfo = new CachedEnvDao.CachedEnvInfo(
            envId, workflowName, disk.id(), CachedEnvStatus.PREPARING, dockerImage, yamlConfig
        );
        cachedEnvDao.insertEnv(envInfo);
        return new CachedEnv(envInfo, disk);
    }

}
