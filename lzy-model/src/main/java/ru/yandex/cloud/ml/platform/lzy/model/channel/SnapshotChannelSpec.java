package ru.yandex.cloud.ml.platform.lzy.model.channel;

import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

public class SnapshotChannelSpec implements Channel {
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
