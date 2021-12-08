package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.*;

@Entity
@Table(name = "snapshot_owner")
public class SnapshotOwnerModel {
    @Id
    @Column(name="snapshot_id", nullable = false)
    private String snapshotId;

    @Column(name = "owner_id", nullable = false)
    private String ownerId;

    public SnapshotOwnerModel(String snapshotId, String ownerId) {
        this.snapshotId = snapshotId;
        this.ownerId = ownerId;
    }

    public SnapshotOwnerModel() {
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }
}