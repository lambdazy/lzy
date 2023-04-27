package ai.lzy.allocator.disk.impl.yc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record YcDeleteDiskState(
    String ycOperationId,
    String folderId,
    String diskId
) {
    public YcDeleteDiskState withYcOperationId(String ycOperationId) {
        return new YcDeleteDiskState(ycOperationId, folderId, diskId);
    }
}
