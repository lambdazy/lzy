package ai.lzy.allocator.model;

import ai.lzy.v1.VmAllocatorApi;
import jakarta.annotation.Nullable;

import java.util.UUID;

public record DynamicMount(
    String id,
    @Nullable String vmId,
    String clusterId,
    String mountPath,
    String bindPath,
    @Nullable String bindOwner,
    String mountName,
    @Nullable String volumeName,
    @Nullable String volumeClaimName,
    VolumeRequest volumeRequest,
    String mountOperationId,
    @Nullable String unmountOperationId,
    State state,
    String workerId,
    boolean mounted
) {
    public enum State {
        PENDING,
        READY,
        DELETING
    }

    public static DynamicMount createNew(String vmId, String clusterId, String mountName,
                                         String mountPath, String bindPath, @Nullable String bindOwner,
                                         VolumeRequest volumeRequest,
                                         String mountOperationId, String workerId)
    {
        return new DynamicMount(UUID.randomUUID().toString(), vmId, clusterId, mountPath, bindPath, bindOwner,
            mountName, null, null, volumeRequest, mountOperationId, null, State.PENDING, workerId, false);
    }

    public VmAllocatorApi.DynamicMount toProto() {
        var builder = VmAllocatorApi.DynamicMount.newBuilder()
            .setId(id)
            .setMountName(mountName)
            .setMountPath(mountPath)
            .setBindPath(bindPath)
            .setMountOperationId(mountOperationId)
            .setState(state.name())
            .setVolumeRequest(volumeRequest.toProto());
        if (vmId != null) {
            builder.setVmId(vmId);
        }
        if (bindOwner != null) {
            builder.setBindOwner(bindOwner);
        }
        if (volumeName != null) {
            builder.setVolumeName(volumeName);
        }
        if (volumeClaimName != null) {
            builder.setVolumeClaimName(volumeClaimName);
        }
        if (unmountOperationId != null) {
            builder.setUnmountOperationId(unmountOperationId);
        }

        return builder.build();
    }

    public record Update(
        @Nullable String volumeName,
        @Nullable String volumeClaimName,
        @Nullable State state,
        @Nullable String unmountOperationId,
        @Nullable Boolean mounted
    ) {
        public boolean isEmpty() {
            return volumeName == null && volumeClaimName == null && state == null && unmountOperationId == null &&
                mounted == null;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String volumeName;
            private String volumeClaimName;
            private State state;
            private String unmountOperationId;
            private Boolean mounted;

            public Builder volumeName(String volumeName) {
                this.volumeName = volumeName;
                return this;
            }

            public Builder volumeClaimName(String volumeClaimName) {
                this.volumeClaimName = volumeClaimName;
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

            public Builder mounted(Boolean mounted) {
                this.mounted = mounted;
                return this;
            }

            public Update build() {
                return new Update(volumeName, volumeClaimName, state, unmountOperationId, mounted);
            }
        }
    }
}
