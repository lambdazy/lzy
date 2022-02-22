package ru.yandex.cloud.ml.platform.lzy.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

public class ContextImpl implements Context {

    private final Env env;
    private final Provisioning provisioning;
    private final SnapshotMeta meta;
    private final List<SlotAssignment> assignments;

    public ContextImpl(Env env, Provisioning provisioning,
                       SnapshotMeta meta,
                       Stream<SlotAssignment> assignments) {
        this.env = env;
        this.provisioning = provisioning;
        this.meta = meta;
        this.assignments = assignments.collect(Collectors.toList());
    }

    @Override
    public Env env() {
        return env;
    }

    @Override
    public Provisioning provisioning() {
        return provisioning;
    }

    @Override
    public SnapshotMeta meta() {
        return meta;
    }

    @Override
    public Stream<SlotAssignment> assignments() {
        return assignments.stream();
    }
}
