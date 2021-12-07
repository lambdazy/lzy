package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "snapshot_entry")
@IdClass(SnapshotEntryModel.SnapshotEntryPk.class)
public class SnapshotEntryModel {
    @Id
    @Column(name="snapshot_id", nullable = false)
    private String snapshotId;

    @Id
    @Column(name = "entry_id", nullable = false)
    private String entryId;

    @Column(name = "storage_uri", nullable = false)
    private String storageUri;

    @Column(name = "empty_content", nullable = false)
    private boolean emptyContent;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false)
    private SnapshotEntryStatus.State entryState;

    @ManyToOne()
    @JoinColumn(name="snapshot_id", nullable=false, insertable = false, updatable = false)
    private SnapshotModel snapshotState;

    public SnapshotEntryModel(String snapshotId, String entryId, String storageUri,
                              boolean emptyContent, SnapshotEntryStatus.State entryStatus) {
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.storageUri = storageUri;
        this.emptyContent = emptyContent;
        this.entryState = entryStatus;
    }

    public SnapshotEntryModel() {
    }

    public SnapshotEntryStatus.State getEntryState() {
        return entryState;
    }

    public void setEntryState(SnapshotEntryStatus.State status) {
        this.entryState = status;
    }

    public String getStorageUri() {
        return storageUri;
    }

    public void setStorageUri(String storageUri) {
        this.storageUri = storageUri;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public boolean isEmptyContent() {
        return emptyContent;
    }

    public void setEmptyContent(boolean emptyContent) {
        this.emptyContent = emptyContent;
    }

    public static class SnapshotEntryPk implements Serializable {
        protected String snapshotId;
        protected String entryId;

        public SnapshotEntryPk(String snapshotId, String entryId) {
            this.snapshotId = snapshotId;
            this.entryId = entryId;
        }

        public SnapshotEntryPk() {};

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SnapshotEntryPk storagePk = (SnapshotEntryPk) o;
            return snapshotId.equals(storagePk.snapshotId) &&
                    entryId.equals(storagePk.entryId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, entryId);
        }
    }
}