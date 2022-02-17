package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;

@Entity
@Table(name = "snapshot")
public class SnapshotModel {

    @Id
    @Column(name = "snapshot_id", nullable = false)
    private String snapshotId;

    @Enumerated(EnumType.STRING)
    @Column(name = "snapshot_state", nullable = false)
    private SnapshotStatus.State snapshotState;

    @Column(name = "uid", nullable = false)
    private String uid;

    public SnapshotModel(String snapshotId, SnapshotStatus.State snapshotState, String uid) {
        this.snapshotId = snapshotId;
        this.snapshotState = snapshotState;
        this.uid = uid;
    }

    public SnapshotModel() {
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public SnapshotStatus.State getSnapshotState() {
        return snapshotState;
    }

    public void setSnapshotState(SnapshotStatus.State snapshotState) {
        this.snapshotState = snapshotState;
    }

    public String getUid() {
        return uid;
    }
}