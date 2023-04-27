package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record YcCloneDiskState(
    String ycCreateSnapshotOperationId,
    String ycCreateDiskOperationId,
    String ycDeleteSnapshotOperationId,
    String folderId,
    @Nullable String snapshotId,
    Disk originDisk,
    DiskSpec newDiskSpec,
    DiskMeta newDiskMeta,
    @Nullable String newDiskId
) {

    public YcCloneDiskState withCreateSnapshotOperationId(String ycCreateSnapshotOperationId) {
        return new YcCloneDiskState(ycCreateSnapshotOperationId, ycCreateDiskOperationId, ycDeleteSnapshotOperationId,
            folderId, snapshotId, originDisk, newDiskSpec, newDiskMeta, newDiskId);
    }

    public YcCloneDiskState withSnapshotId(String snapshotId) {
        return new YcCloneDiskState(ycCreateSnapshotOperationId, ycCreateDiskOperationId, ycDeleteSnapshotOperationId,
            folderId, snapshotId, originDisk, newDiskSpec, newDiskMeta, newDiskId);
    }

    public YcCloneDiskState withCreateDiskOperationId(String ycCreateDiskOperationId) {
        return new YcCloneDiskState(ycCreateSnapshotOperationId, ycCreateDiskOperationId, ycDeleteSnapshotOperationId,
            folderId, snapshotId, originDisk, newDiskSpec, newDiskMeta, newDiskId);
    }

    public YcCloneDiskState withNewDiskId(String newDiskId) {
        return new YcCloneDiskState(ycCreateSnapshotOperationId, ycCreateDiskOperationId, ycDeleteSnapshotOperationId,
            folderId, snapshotId, originDisk, newDiskSpec, newDiskMeta, newDiskId);
    }

    public YcCloneDiskState withDeleteSnapshotOperationId(String ycDeleteSnapshotOperationId) {
        return new YcCloneDiskState(ycCreateSnapshotOperationId, ycCreateDiskOperationId, ycDeleteSnapshotOperationId,
            folderId, snapshotId, originDisk, newDiskSpec, newDiskMeta, newDiskId);
    }
}
