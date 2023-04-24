package ai.lzy.allocator.volume;

import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;

import javax.annotation.Nullable;

public interface VolumeManager {
    Volume create(String clusterId, VolumeRequest volumeRequest);

    VolumeClaim createClaim(String clusterId, Volume volume);

    @Nullable
    Volume get(String clusterId, String volumeName);

    @Nullable
    VolumeClaim getClaim(String clusterId, String volumeClaimId);

    void delete(String clusterId, String volumeName);

    void deleteClaim(String clusterId, String volumeClaimName);
}
