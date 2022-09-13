package ai.lzy.channelmanager.channel;

import ai.lzy.model.data.DataSchema;

public interface ChannelSpec {
    String name();
    DataSchema contentType();
}
