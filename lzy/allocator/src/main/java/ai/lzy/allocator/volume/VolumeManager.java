package ai.lzy.allocator.volume;

import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;

import javax.annotation.Nullable;

public interface VolumeManager {
    Volume create(VolumeRequest resourceVolumeType) throws NotFoundException;
    VolumeClaim createClaim(Volume volume);

    @Nullable
    Volume get(String volumeName);

    @Nullable
    VolumeClaim getClaim(String volumeClaimId);

    void delete(String volumeName);
    void deleteClaim(String volumeClaimName);
}
