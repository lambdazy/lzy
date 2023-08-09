package ai.lzy.allocator.volume;

import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import jakarta.annotation.Nullable;

import java.time.Duration;

public interface VolumeManager {
    Volume create(String clusterId, VolumeRequest volumeRequest) throws RetryLaterException;

    VolumeClaim createClaim(String clusterId, Volume volume);

    @Nullable
    Volume get(String clusterId, String volumeName);

    @Nullable
    VolumeClaim getClaim(String clusterId, String volumeClaimId);

    void delete(String clusterId, String volumeName);

    void deleteClaim(String clusterId, String volumeClaimName);

    class RetryLaterException extends Exception {
        private final Duration delay;

        public RetryLaterException(String message, Duration delay) {
            super(message);
            this.delay = delay;
        }

        public Duration delay() {
            return delay;
        }
    }
}
