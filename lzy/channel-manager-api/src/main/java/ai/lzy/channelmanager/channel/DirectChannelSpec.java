package ai.lzy.channelmanager.channel;

import ai.lzy.model.DataScheme;

@Deprecated
public record DirectChannelSpec(
    String name,
    DataScheme contentType
) implements ChannelSpec { }

