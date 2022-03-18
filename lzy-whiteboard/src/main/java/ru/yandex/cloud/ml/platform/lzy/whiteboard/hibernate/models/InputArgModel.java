package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "input_arg")
public class InputArgModel extends ArgModelBase {
    @Column(name = "hash")
    private String hash;

    public InputArgModel(String snapshotId, String entryId, Integer executionId, String name, String hash) {
        super(snapshotId, entryId, executionId, name);
        this.hash = hash;
    }

    public InputArgModel() {}

    public String hash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }
}
