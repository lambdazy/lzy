package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

public interface ExecutionArg {
    String name();

    String snapshotId();

    String entryId();
}
