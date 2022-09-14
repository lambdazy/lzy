package ai.lzy.channelmanager.channel;

import ai.lzy.model.DataScheme;

public record DirectChannelSpec(
    String name,
    DataScheme contentType
) implements ChannelSpec { }

