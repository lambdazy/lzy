package ru.yandex.cloud.ml.platform.lzy.whiteboard.hibernate.models;

import java.io.Serializable;
import java.util.Objects;

public class ArgPk implements Serializable {

    private String snapshotId;
    private String entryId;
    private Integer executionId;
    private String name;

    public ArgPk() {}

    public ArgPk(String snapshotId, String entryId, Integer executionId, String name) {
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        this.executionId = executionId;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArgPk argPk = (ArgPk) o;
        return Objects.equals(snapshotId, argPk.snapshotId)
            && Objects.equals(entryId, argPk.entryId)
            && Objects.equals(executionId, argPk.executionId)
            && Objects.equals(name, argPk.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, entryId, executionId, name);
    }
}
