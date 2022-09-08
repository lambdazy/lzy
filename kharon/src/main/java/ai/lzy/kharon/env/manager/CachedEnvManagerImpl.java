package ai.lzy.kharon.env.manager;

import ai.lzy.disk.model.Disk;
import ai.lzy.disk.model.DiskType;
import ai.lzy.disk.model.EntityNotFoundException;
import ai.lzy.disk.model.grpc.DiskClient;
import ai.lzy.disk.model.grpc.GrpcConverter;
import ai.lzy.kharon.env.CachedEnv;
import ai.lzy.kharon.env.CachedEnvStatus;
import ai.lzy.kharon.env.dao.CachedEnvDao;
import ai.lzy.v1.disk.LD;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(beans = DiskClient.class)
@Requires(beans = CachedEnvDao.class)
public class CachedEnvManagerImpl implements CachedEnvManager {

    private static final Logger LOG = LogManager.getLogger(CachedEnvManagerImpl.class);

    @Inject
    private CachedEnvDao cachedEnvDao;
    @Inject
    private DiskClient diskClient;

    @Override
    public CachedEnv saveEnvConfig(
        String userId,
        String workflowName,
        String dockerImage,
        String condaYaml,
        DiskType diskType
    ) {
        final CachedEnv existedEnv = findConfigRelatedEnv(userId, workflowName, dockerImage, condaYaml, diskType);
        if (existedEnv != null) {
            return existedEnv;
        }

        clearWorkflowEnvs(userId, workflowName);

        final CachedEnv newEnv = createCachedEnv(userId, workflowName, dockerImage, condaYaml, diskType);

        return newEnv;
    }

    @Override
    public void markEnvReady(String userId, String workflowName, String diskId) {
        final var envInfo = cachedEnvDao.findEnv(userId, workflowName, diskId);
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
        String userId,
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    ) {
        final Map<String, Disk> userDisks = diskClient.listUserDisks(userId).stream()
            .collect(Collectors.toMap(LD.Disk::getId, GrpcConverter::from));

        final List<CachedEnvDao.CachedEnvInfo> configRelatedEnvs = cachedEnvDao.listEnvs(userId, workflowName)
            .filter(env -> env.dockerImage().equals(dockerImage))
            .filter(env -> env.yamlConfig().equals(yamlConfig))
            .filter(env -> userDisks.containsKey(env.diskId()))
            .filter(env -> diskType.equals(userDisks.get(env.diskId()).type()))
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

        return new CachedEnv(envInfo, userDisks.get(envInfo.diskId()));
    }

    private void clearWorkflowEnvs(String userId, String workflowName) {
        final List<CachedEnvDao.CachedEnvInfo> workflowEnvs = cachedEnvDao.listEnvs(userId, workflowName).toList();
        cachedEnvDao.deleteWorkflowEnvs(userId, workflowName);

        final List<String> aliveDisks = new ArrayList<>();
        AtomicReference<RuntimeException> failedDiskDeletion = new AtomicReference<>();
        workflowEnvs.forEach(env -> {
            try {
                diskClient.deleteDisk(userId, env.diskId());
            } catch (EntityNotFoundException e) {
                LOG.warn("Disk {} of env {} not found", env.diskId(), env.envId());
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
        String userId,
        String workflowName,
        String dockerImage,
        String yamlConfig,
        DiskType diskType
    ) {
        final String envId = "env-" + UUID.randomUUID();
        final Disk disk = GrpcConverter.from(diskClient.createDisk(
            userId, "env-disk", GrpcConverter.to(diskType), 0
        ));
        final CachedEnvDao.CachedEnvInfo envInfo = new CachedEnvDao.CachedEnvInfo(
            envId, userId, workflowName, disk.id(), CachedEnvStatus.PREPARING, dockerImage, yamlConfig
        );
        cachedEnvDao.insertEnv(envInfo);
        return new CachedEnv(envInfo, disk);
    }

}
