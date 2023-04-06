package ai.lzy.allocator.model;

import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public record DynamicMount(
    String id,
    @Nullable String vmId,
    String clusterId,
    String mountPath,
    String mountName,
    @Nullable String volumeClaimId,
    DiskVolumeDescription volumeDescription,
    String mountOperationId,
    @Nullable String unmountOperationId,
    State state,
    String workerId
) {
    public enum State {
        PENDING,
        READY,
        DELETING
    }

    public static DynamicMount createNew(String vmId, String clusterId, String mountName, String mountPath,
                                         DiskVolumeDescription volumeDescription, String mountOperationId,
                                         String workerId)
    {
        return new DynamicMount(UUID.randomUUID().toString(), vmId, clusterId, mountPath, mountName, null,
            volumeDescription, mountOperationId, null, State.PENDING, workerId);
    }

    public DynamicMount apply(Update update) {
        var volumeClaimId = update.volumeClaimId() != null ? update.volumeClaimId() : volumeClaimId();
        var state = update.state() != null ? update.state() : state();
        var unmountOperationId = update.unmountOperationId() != null ? update.unmountOperationId()
            : unmountOperationId();
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state, workerId);
    }

    public record Update(
        @Nullable String volumeClaimId,
        @Nullable State state,
        @Nullable String unmountOperationId
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String volumeClaimId;
            private State state;
            private String unmountOperationId;

            public Builder volumeClaimId(String volumeClaimId) {
                this.volumeClaimId = volumeClaimId;
                return this;
            }

            public Builder state(State state) {
                this.state = state;
                return this;
            }

            public Builder unmountOperationId(String unmountOperationId) {
                this.unmountOperationId = unmountOperationId;
                return this;
            }

            public Update build() {
                return new Update(volumeClaimId, state, unmountOperationId);
            }
        }
    }
}
