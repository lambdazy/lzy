package ai.lzy.channelmanager.channel;

import ai.lzy.model.DataScheme;

import java.net.URI;

public record SnapshotChannelSpec(
    String userId,
    String name,
    DataScheme contentType,
    String snapshotId,
    String entryId,
    URI whiteboardAddress
) implements ChannelSpec
{ }
