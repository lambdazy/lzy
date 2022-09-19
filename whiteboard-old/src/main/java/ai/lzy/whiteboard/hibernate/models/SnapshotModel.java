package ai.lzy.whiteboard.hibernate.models;

import ai.lzy.whiteboard.model.SnapshotStatus;

import java.util.Date;
import javax.annotation.Nullable;
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

    @Column(name = "uid", nullable = false)
    private String uid;

    @Column(name = "creation_date_UTC", nullable = false)
    private Date creationDateUTC;

    @Column(name = "workflow_name", nullable = false)
    private String workflowName;

    @Column(name = "parent_snapshot_id")
    private String parentSnapshotId;

    public SnapshotModel(
        String snapshotId,
        SnapshotStatus.State snapshotState,
        String uid,
        Date creationDateUTC,
        String workflowName,
        @Nullable String parentSnapshotId
    ) {
        this.snapshotId = snapshotId;
        this.snapshotState = snapshotState;
        this.uid = uid;
        this.creationDateUTC = creationDateUTC;
        this.workflowName = workflowName;
        this.parentSnapshotId = parentSnapshotId;
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

    public Date creationDateUTC() {
        return creationDateUTC;
    }

    public String workflowName() {
        return workflowName;
    }

    @Nullable
    public String parentSnapshotId() {
        return parentSnapshotId;
    }

    public String toString() {
        return "Snapshot id: " + snapshotId + ", state: " + snapshotState.toString() + ", uid: " + uid;
    }
}
