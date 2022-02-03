package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.WhiteboardStatus;

import javax.persistence.*;


@Entity
@Table(name = "whiteboard")
public class WhiteboardModel {
    @Id
    @Column(name = "wb_id", nullable = false)
    private String wbId;

    @Enumerated(EnumType.STRING)
    @Column(name = "wb_state", nullable = false)
    private WhiteboardStatus.State wbState;

    @Column(name = "snapshot_id", nullable = false)
    private String snapshotId;

    @Column(name = "namespace")
    private String namespace;

    public WhiteboardModel(String wbId, WhiteboardStatus.State wbStatus, String snapshotId, String namespace) {
        this.wbId = wbId;
        this.wbState = wbStatus;
        this.snapshotId = snapshotId;
        this.namespace = namespace;
    }

    public WhiteboardModel() {
    }

    public void setWbState(WhiteboardStatus.State wbStatus) {
        this.wbState = wbStatus;
    }

    public WhiteboardStatus.State getWbState() {
        return wbState;
    }

    public String getWbId() {
        return wbId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getNamespace() {
        return namespace;
    }
}