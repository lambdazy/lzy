package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;

import javax.persistence.*;

@Entity
@Table(name = "snapshot")
public class SnapshotModel {
    @Id
    @Column(name = "snapshot_id", nullable = false)
    private String snapshotId;

    @Enumerated(EnumType.STRING)
    @Column(name = "snapshot_state", nullable = false)
    private SnapshotStatus.State snapshotState;

    public SnapshotModel(String snapshotId, SnapshotStatus.State snapshotState) {
        this.snapshotId = snapshotId;
        this.snapshotState = snapshotState;
    }

    public SnapshotModel() {
    }

    public void setSnapshotState(SnapshotStatus.State snapshotState) {
        this.snapshotState = snapshotState;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public SnapshotStatus.State getSnapshotState() {
        return snapshotState;
    }
}