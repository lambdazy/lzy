package ai.lzy.whiteboard.hibernate.models;

import javax.persistence.*;

@MappedSuperclass
@IdClass(ArgPk.class)
public class ArgModelBase {
    @Id
    @Column(name = "snapshot_id")
    private String snapshotId;

    @Id
    @Column(name = "entry_id")
    private String entryId;

    @Id
    @Column(name = "execution_id")
    private Integer executionId;

    @Id
    @Column(name = "name")
    private String name;

    @ManyToOne
    @JoinColumns({
        @JoinColumn(name = "snapshot_id", referencedColumnName = "snapshot_id", insertable = false, updatable = false),
        @JoinColumn(name = "entry_id", referencedColumnName = "entry_id", insertable = false, updatable = false)
    })
    private SnapshotEntryModel entry;

    @ManyToOne
    @JoinColumn(name = "execution_id", referencedColumnName = "execution_id", insertable = false, updatable = false)
    private ExecutionModel execution;

    @ManyToOne
    @JoinColumn(name = "snapshot_id", referencedColumnName = "snapshot_id", insertable = false, updatable = false)
    private SnapshotModel snapshotModel;

    public ArgModelBase(String snapshotId, String entryId, Integer executionId, String name) {
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.executionId = executionId;
        this.name = name;
    }

    public ArgModelBase() {}

    public String snapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String entryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public Integer executionId() {
        return executionId;
    }

    public void setExecutionId(Integer executionId) {
        this.executionId = executionId;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExecutionModel execution() {
        return execution;
    }

}
