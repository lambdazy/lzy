package ai.lzy.allocator.model;

import jakarta.annotation.Nullable;

import java.util.UUID;

public record DynamicMount(
    String id,
    String vmId,
    String clusterId,
    String mountPath,
    @Nullable String mountName,
    @Nullable String volumeClaimId,
    DiskVolumeDescription volumeDescription,
    String mountOperationId,
    @Nullable String unmountOperationId,
    State state
) {
    public enum State {
        PENDING,
        READY,
        DELETING
    }

    public static DynamicMount createNew(String vmId, String clusterId, String mountPath,
                                         DiskVolumeDescription volumeDescription, String mountOperationId)
    {
        return new DynamicMount(UUID.randomUUID().toString(), vmId, clusterId, mountPath, null, null,
            volumeDescription, mountOperationId, null, State.PENDING);
    }

    public DynamicMount apply(Update update) {
        var mountName = update.mountName() != null ? update.mountName() : mountName();
        var volumeClaimId = update.volumeClaimId() != null ? update.volumeClaimId() : volumeClaimId();
        var state = update.state() != null ? update.state() : state();
        var unmountOperationId = update.unmountOperationId() != null ? update.unmountOperationId()
            : unmountOperationId();
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state);
    }

    public record Update(
        @Nullable String volumeClaimId,
        @Nullable State state,
        @Nullable String mountName,
        @Nullable String unmountOperationId
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String volumeClaimId;
            private State state;
            private String mountName;
            private String unmountOperationId;

            public Builder volumeClaimId(String volumeClaimId) {
                this.volumeClaimId = volumeClaimId;
                return this;
            }

            public Builder state(State state) {
                this.state = state;
                return this;
            }

            public Builder mountName(String mountName) {
                this.mountName = mountName;
                return this;
            }

            public Builder unmountOperationId(String unmountOperationId) {
                this.unmountOperationId = unmountOperationId;
                return this;
            }

            public Update build() {
                return new Update(volumeClaimId, state, mountName, unmountOperationId);
            }
        }
    }
}