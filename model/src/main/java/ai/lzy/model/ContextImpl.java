package ai.lzy.model;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ContextImpl implements Context {

    private final Env env;
    private final Provisioning provisioning;
    private final List<SlotAssignment> assignments;

    public ContextImpl(Env env, Provisioning provisioning,
                       Stream<SlotAssignment> assignments) {
        this.env = env;
        this.provisioning = provisioning;
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
    public Stream<SlotAssignment> assignments() {
        return assignments.stream();
    }
}
