package ai.lzy.channelmanager.channel;

import ai.lzy.model.DataScheme;

public interface ChannelSpec {
    String name();
    DataScheme contentType();
}
