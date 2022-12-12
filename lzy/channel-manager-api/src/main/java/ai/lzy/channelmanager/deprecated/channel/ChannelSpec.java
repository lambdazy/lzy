package ai.lzy.channelmanager.deprecated.channel;

import ai.lzy.model.DataScheme;

@Deprecated
public interface ChannelSpec {
    String name();
    DataScheme contentType();
}
