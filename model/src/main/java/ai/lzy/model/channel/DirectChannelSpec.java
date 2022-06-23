package ai.lzy.model.channel;

import ai.lzy.model.data.DataSchema;

public class DirectChannelSpec implements ChannelSpec {
    private final String name;
    private final DataSchema contentType;

    public DirectChannelSpec(String name, DataSchema contentType) {
        this.name = name;
        this.contentType = contentType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataSchema contentType() {
        return contentType;
    }
}
