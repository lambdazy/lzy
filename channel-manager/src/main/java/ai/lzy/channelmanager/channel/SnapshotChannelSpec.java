package ai.lzy.channelmanager.channel;

import ai.lzy.model.data.DataSchema;
import java.net.URI;

public record SnapshotChannelSpec(
    String name,
    DataSchema contentType,
    String snapshotId,
    String entryId,
    URI whiteboardAddress
) implements ChannelSpec { }
