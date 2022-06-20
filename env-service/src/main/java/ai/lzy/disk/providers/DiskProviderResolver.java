package ai.lzy.disk.providers;

import ai.lzy.common.s3.S3ClientFactory;
import jakarta.inject.Inject;
import ai.lzy.disk.DiskType;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskProviderResolver {

    private static final Logger LOG = LogManager.getLogger(DiskProviderResolver.class);

    @Inject
    List<DiskProvider> providers;

    public DiskProvider getProvider(DiskType diskType) {
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
