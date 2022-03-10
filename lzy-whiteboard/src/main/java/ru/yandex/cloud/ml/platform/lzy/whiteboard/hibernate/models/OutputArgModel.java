package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.Entity;
import javax.persistence.Table;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.ExecutionArg;

@Entity
@Table(name = "output_arg")
public class OutputArgModel extends ArgModelBase {
    public OutputArgModel(String snapshotId, String entryId, Integer executionId, String name) {
        super(snapshotId, entryId, executionId, name);
    }

    public OutputArgModel() {
        super();
    }
}
