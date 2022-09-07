package ai.lzy.disk.providers;

import jakarta.inject.Inject;
import ai.lzy.disk.model.DiskType;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskStorageProviderResolver {

    private static final Logger LOG = LogManager.getLogger(DiskStorageProviderResolver.class);

    @Inject
    List<DiskStorageProvider> providers;

    public DiskStorageProvider getProvider(DiskType diskType) {
        return providers.stream()
            .filter(provider -> provider.getType().equals(diskType))
            .findAny()
            .orElseThrow(() -> {
                String errorMessage = "Failed to resolve disk provider of type " + diskType;
                LOG.error(errorMessage);
                return new IllegalArgumentException(errorMessage);
            });
    }

}
