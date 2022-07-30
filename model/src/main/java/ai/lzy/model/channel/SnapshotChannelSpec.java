package ai.lzy.model.channel;

import ai.lzy.model.data.DataSchema;
import java.net.URI;

public class SnapshotChannelSpec implements ChannelSpec {
    private final String name;
    private final DataSchema contentType;
    private final String snapshotId;
    private final String entryId;
    private final URI whiteboardAddress;

    public SnapshotChannelSpec(
        String name,
        DataSchema contentType,
        String snapshotId,
        String entryId,
        URI whiteboardAddress
    ) {
        this.name = name;
        this.contentType = contentType;
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.whiteboardAddress = whiteboardAddress;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataSchema contentType() {
        return contentType;
    }

    public String snapshotId() {
        return snapshotId;
    }

    public String entryId() {
        return entryId;
    }

    public URI getWhiteboardAddress() {
        return whiteboardAddress;
    }
}
