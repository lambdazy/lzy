package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "whiteboard_field")
@IdClass(WhiteboardFieldModel.WhiteboardFieldPk.class)
public class WhiteboardFieldModel {
    @Id
    @Column(name="wb_id")
    private String wbId;

    @Id
    @Column(name = "field_name", nullable = false)
    private String fieldName;

    @Column(name = "entry_id")
    private String entryId;

    @ManyToOne
    @JoinColumn(name="wb_id", nullable=false, insertable = false, updatable = false)
    private WhiteboardModel whiteboardModel;

    public WhiteboardFieldModel(String wbId, String fieldName, String entryId) {
        this.wbId = wbId;
        this.fieldName = fieldName;
        this.entryId = entryId;
    }

    public WhiteboardFieldModel() {
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getEntryId() {
        return entryId;
    }

    public String getSnapshotId() {
        return whiteboardModel.getSnapshotId();
    }

    public static class WhiteboardFieldPk implements Serializable {
        protected String wbId;
        protected String fieldName;

        public WhiteboardFieldPk(String wbId, String fieldName) {
            this.wbId = wbId;
            this.fieldName = fieldName;
        }

        public WhiteboardFieldPk() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WhiteboardFieldModel.WhiteboardFieldPk storagePk = (WhiteboardFieldModel.WhiteboardFieldPk) o;
            return wbId.equals(storagePk.wbId) &&
                    fieldName.equals(storagePk.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(wbId, fieldName);
        }
    }
}