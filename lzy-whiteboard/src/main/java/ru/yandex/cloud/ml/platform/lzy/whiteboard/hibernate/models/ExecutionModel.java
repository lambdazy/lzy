package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import java.net.URI;
import java.util.Set;
import java.util.stream.Stream;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;

@Entity
@Table(name = "execution")
public class ExecutionModel {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "execution_id")
    private int executionId;

    @ManyToOne
    @JoinColumn(name = "snapshot_id", nullable = false, insertable = false, updatable = false)
    private SnapshotModel snapshot;

    @Column(name = "snapshot_id")
    private String snapshotId;


    @Column(name = "name", nullable = false)
    private String name;

    @OneToMany(mappedBy = "execution")
    private Set<InputArgModel> inputs;

    @OneToMany(mappedBy = "execution")
    private Set<OutputArgModel> outputs;

    public ExecutionModel(String snapshotId, String name) {
        this.snapshotId = snapshotId;
        this.name = name;
    }

    public ExecutionModel() {}

    public int executionId() {
        return executionId;
    }

    public SnapshotModel getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(SnapshotModel snapshot) {
        this.snapshot = snapshot;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public Stream<OutputArgModel> outputs() {
        return outputs.stream();
    }

    public Stream<InputArgModel> inputs() {
        return inputs.stream();
    }

    public Snapshot snapshot() {
        return new Snapshot.Impl(URI.create(snapshot.getSnapshotId()), URI.create(snapshot.getUid()),
            snapshot.creationDateUTC(), snapshot.workflowName(), snapshot.parentSnapshotId());
    }

    public String snapshotId() {
        return snapshotId;
    }
}
