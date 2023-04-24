package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record YcCreateDiskState(
    String ycOperationId,
    String folderId,
    @Nullable String snapshotId,
    DiskSpec spec,
    DiskMeta meta
) {
    public YcCreateDiskState withYcOperationId(String ycOperationId) {
        return new YcCreateDiskState(ycOperationId, folderId, snapshotId, spec, meta);
    }
}
