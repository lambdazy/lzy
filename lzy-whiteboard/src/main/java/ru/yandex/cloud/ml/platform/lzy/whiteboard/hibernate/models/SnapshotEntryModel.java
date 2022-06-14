package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;

@Entity
@Table(name = "snapshot_entry")
@IdClass(SnapshotEntryModel.SnapshotEntryPk.class)
public class SnapshotEntryModel {

    @Id
    @Column(name = "snapshot_id", nullable = false)
    private String snapshotId;

    @Id
    @Column(name = "entry_id", nullable = false)
    private String entryId;

    @Column(name = "storage_uri")
    private String storageUri;

    @Column(name = "type_description")
    private String typeDescription;

    @Column(name = "type_of_scheme")
    private String typeOfScheme;

    @Column(name = "empty", nullable = false)
    private boolean empty;

    @Enumerated(EnumType.STRING)
    @Column(name = "state")
    private SnapshotEntryStatus.State entryState;

    public SnapshotEntryModel(String snapshotId, String entryId, String storageUri,
        boolean emptyContent, SnapshotEntryStatus.State entryStatus) {
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.storageUri = storageUri;
        this.empty = emptyContent;
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

    @Nullable
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

    public String getTypeDescription() {
        return typeDescription;
    }

    public void setTypeDescription(String typeDescription) {
        this.typeDescription = typeDescription;
    }

    public String getTypeOfScheme() {
        return typeOfScheme;
    }

    public void setTypeOfScheme(String typeOfScheme) {
        this.typeOfScheme = typeOfScheme;
    }

    public boolean isEmpty() {
        return empty;
    }

    public void setEmpty(boolean emptyContent) {
        this.empty = emptyContent;
    }

    public static class SnapshotEntryPk implements Serializable {

        protected String snapshotId;
        protected String entryId;

        public SnapshotEntryPk(String snapshotId, String entryId) {
            this.snapshotId = snapshotId;
            this.entryId = entryId;
        }

        public SnapshotEntryPk() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotEntryPk storagePk = (SnapshotEntryPk) o;
            return snapshotId.equals(storagePk.snapshotId)
                && entryId.equals(storagePk.entryId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, entryId);
        }
    }
}