package ai.lzy.whiteboard.hibernate.models;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "whiteboard_tag")
@IdClass(WhiteboardTagModel.WhiteboardTagPk.class)
public class WhiteboardTagModel {

    @Id
    @Column(name = "wb_id")
    private String wbId;

    @Id
    @Column(name = "tag", nullable = false)
    private String tag;

    @ManyToOne
    @JoinColumn(name = "wb_id", nullable = false, insertable = false, updatable = false)
    private WhiteboardModel whiteboardModel;

    public WhiteboardTagModel(String wbId, String tag) {
        this.wbId = wbId;
        this.tag = tag;
    }

    public WhiteboardTagModel() {
    }

    public String getTag() {
        return tag;
    }

    public String getSnapshotId() {
        return whiteboardModel.getSnapshotId();
    }

    public static class WhiteboardTagPk implements Serializable {

        protected String wbId;
        protected String tag;

        public WhiteboardTagPk(String wbId, String tag) {
            this.wbId = wbId;
            this.tag = tag;
        }

        public WhiteboardTagPk() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WhiteboardTagModel.WhiteboardTagPk storagePk = (WhiteboardTagModel.WhiteboardTagPk) o;
            return wbId.equals(storagePk.wbId)
                && tag.equals(storagePk.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(wbId, tag);
        }
    }
}