package ai.lzy.channelmanager.deprecated.channel;

import ai.lzy.model.DataScheme;

import java.net.URI;

@Deprecated
public record SnapshotChannelSpec(
    String userId,
    String name,
    DataScheme contentType,
    String snapshotId,
    String entryId,
    URI whiteboardAddress
) implements ChannelSpec { }
