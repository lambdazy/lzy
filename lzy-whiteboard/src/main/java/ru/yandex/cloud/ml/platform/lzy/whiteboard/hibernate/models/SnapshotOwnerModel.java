package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.*;

@Entity
@Table(name = "snapshot_owner")
public class SnapshotOwnerModel {
    @Id
    @Column(name = "snapshot_id", nullable = false)
    private String spId;

    @Column(name = "uid", nullable = false)
    private String uid;

    public SnapshotOwnerModel(String spId, String uid) {
        this.spId = spId;
        this.uid = uid;
    }

    public SnapshotOwnerModel() {
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
