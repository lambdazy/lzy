package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "entry_dependencies")
@IdClass(EntryDependenciesModel.EntryDependenciesPk.class)
public class EntryDependenciesModel {
    @Id
    @Column(name="snapshot_id")
    private String snapshotId;

    @Id
    @Column(name = "entry_id_from")
    private String entryIdFrom;

    @Id
    @Column(name = "entry_id_to", nullable = false)
    private String entryIdTo;

    public EntryDependenciesModel(String snapshotId, String entryIdFrom, String entryIdTo) {
        this.snapshotId = snapshotId;
        this.entryIdFrom = entryIdFrom;
        this.entryIdTo = entryIdTo;
    }

    public EntryDependenciesModel() {
    }

    public String getEntryIdFrom() {
        return entryIdFrom;
    }

    public static class EntryDependenciesPk implements Serializable {
        protected String snapshotId;
        protected String entryIdFrom;
        protected String entryIdTo;

        public EntryDependenciesPk(String wbId, String fieldName, String slotName) {
            this.snapshotId = wbId;
            this.entryIdFrom = fieldName;
            this.entryIdTo = slotName;
        }

        public EntryDependenciesPk() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EntryDependenciesPk storagePk = (EntryDependenciesPk) o;
            return snapshotId.equals(storagePk.snapshotId) &&
                    entryIdFrom.equals(storagePk.entryIdFrom) &&
                    entryIdTo.equals(storagePk.entryIdTo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, entryIdFrom, entryIdTo);
        }
    }
}