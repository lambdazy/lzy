package ai.lzy.allocator.volume;

import ai.lzy.allocator.disk.exceptions.NotFoundException;

import javax.annotation.Nullable;

public interface VolumeManager {
    Volume create(DiskVolumeDescription diskVolumeType) throws NotFoundException;
    VolumeClaim createClaim(Volume volume);

    @Nullable
    Volume get(String volumeName);

    @Nullable
    VolumeClaim getClaim(String volumeClaimId);

    void delete(String volumeName);
    void deleteClaim(String volumeClaimName);
}
