package ai.lzy.allocator.model;

import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VolumeApi;
import jakarta.annotation.Nullable;

import java.util.UUID;

public record DynamicMount(
    String id,
    @Nullable String vmId,
    String clusterId,
    String mountPath,
    String mountName,
    @Nullable String volumeName,
    @Nullable String volumeClaimName,
    VolumeRequest.ResourceVolumeDescription volumeDescription,
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
                                         VolumeRequest.ResourceVolumeDescription volumeDescription,
                                         String mountOperationId, String workerId)
    {
        return new DynamicMount(UUID.randomUUID().toString(), vmId, clusterId, mountPath, mountName, null, null,
            volumeDescription, mountOperationId, null, State.PENDING, workerId);
    }

    public VmAllocatorApi.DynamicMount toProto() {
        var builder = VmAllocatorApi.DynamicMount.newBuilder()
            .setId(id)
            .setMountName(mountName)
            .setMountPath(mountPath)
            .setMountOperationId(mountOperationId)
            .setState(state.name());
        if (volumeDescription instanceof DiskVolumeDescription diskVolumeDescription) {
            builder.setDiskVolume(VolumeApi.DiskVolumeType.newBuilder()
                .setDiskId(diskVolumeDescription.diskId())
                .setSizeGb(diskVolumeDescription.sizeGb())
                .build());
        }
        if (vmId != null) {
            builder.setVmId(vmId);
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
        @Nullable String unmountOperationId
    ) {
        public boolean isEmpty() {
            return volumeName == null && volumeClaimName == null && state == null && unmountOperationId == null;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String volumeName;
            private String volumeClaimName;
            private State state;
            private String unmountOperationId;

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

            public Update build() {
                return new Update(volumeName, volumeClaimName, state, unmountOperationId);
            }
        }
    }
}
