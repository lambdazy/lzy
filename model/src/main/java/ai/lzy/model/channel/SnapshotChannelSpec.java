package ai.lzy.model.channel;

import ai.lzy.model.data.DataSchema;
import ai.lzy.v1.IAM;

public class SnapshotChannelSpec implements ChannelSpec {
    private final String name;
    private final DataSchema contentType;
    private final String snapshotId;
    private final String entryId;
    private final IAM.Auth auth;

    public SnapshotChannelSpec(String name, DataSchema contentType, String snapshotId, String entryId, IAM.Auth auth) {
        this.name = name;
        this.contentType = contentType;
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.auth = auth;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataSchema contentType() {
        return contentType;
    }

    public String snapshotId() {
        return snapshotId;
    }

    public String entryId() {
        return entryId;
    }

    public IAM.Auth auth() {
        return auth;
    }
}
