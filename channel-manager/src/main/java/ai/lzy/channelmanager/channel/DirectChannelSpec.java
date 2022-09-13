package ai.lzy.channelmanager.channel;

import ai.lzy.model.data.DataSchema;

public record DirectChannelSpec(
    String name,
    DataSchema contentType
) implements ChannelSpec { }

