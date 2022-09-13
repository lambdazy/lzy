package ai.lzy.disk.providers;

import ai.lzy.disk.model.DiskSpec;
import ai.lzy.disk.model.DiskType;
import ai.lzy.disk.model.LocalDirSpec;
import ai.lzy.disk.configs.LocalStorageProviderConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class LocalStorageProvider implements DiskStorageProvider {

    private static final Logger LOG = LogManager.getLogger(LocalStorageProvider.class);
    private static final DiskType PROVIDER_TYPE = DiskType.LOCAL_DIR;

    private final LocalStorageProviderConfig config;

    @Inject
    LocalStorageProvider(LocalStorageProviderConfig config) {
        this.config = config;
    }


    @Override
    public DiskType getType() {
        return PROVIDER_TYPE;
    }

    @Override
    public LocalDirSpec createDisk(String label, String diskId, int diskSizeGb) {
        String folderName = genFolderName(label, diskId);
        Path diskLocation = Path.of(config.disksLocation(), folderName);
        if (!Files.exists(diskLocation)) {
            try {
                Files.createDirectories(diskLocation);
            } catch (IOException e) {
                LOG.error("Failed to create folder for disk {}", folderName);
                throw new RuntimeException(e);
            }
        }
        return new LocalDirSpec(diskSizeGb, diskLocation.toAbsolutePath().toString(), folderName);
    }

    @Override
    public boolean isExistDisk(DiskSpec diskSpec) {
        LocalDirSpec folderSpec = assertDiskSpec(diskSpec);
        return Files.exists(Path.of(folderSpec.fullPath()));
    }

    @Override
    public void deleteDisk(DiskSpec diskSpec) {
        LocalDirSpec folderSpec = assertDiskSpec(diskSpec);
        Path diskLocation = Path.of(folderSpec.fullPath());
        try {
            Files.deleteIfExists(diskLocation);
        } catch (IOException e) {
            LOG.error(
                "Failed to delete folder {} for disk {}",
                folderSpec.fullPath(), folderSpec.folderName()
            );
            throw new RuntimeException(e);
        }
    }

    private String genFolderName(String label, String diskId) {
        return label.replaceAll("[^-a-z0-9]", "-") + "-" + diskId;
    }

    private LocalDirSpec assertDiskSpec(DiskSpec spec) {
        if (!PROVIDER_TYPE.equals(spec.type())) {
            String errorMessage = "Unexpected disk spec of type " + spec.type();
            LOG.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        return (LocalDirSpec) spec;
    }

}
