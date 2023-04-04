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

    public DynamicMount withMountName(String mountName) {
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state);
    }

    public DynamicMount withVolumeClaimId(String volumeClaimId) {
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state);
    }

    public DynamicMount withState(State state) {
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state);
    }

    public DynamicMount withUnmountOperationId(String unmountOperationId) {
        return new DynamicMount(id, vmId, clusterId, mountPath, mountName, volumeClaimId, volumeDescription,
            mountOperationId, unmountOperationId, state);
    }

    public static DynamicMount createNew(String vmId, String clusterId, String mountPath,
                                         DiskVolumeDescription volumeDescription, String mountOperationId)
    {
        return new DynamicMount(UUID.randomUUID().toString(), vmId, clusterId, mountPath, null, null,
            volumeDescription, mountOperationId, null, State.PENDING);
    }
}