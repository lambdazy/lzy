package ai.lzy.env.base;

import ai.lzy.env.Environment;
import ai.lzy.env.aux.CondaPackageRegistry;

public abstract class BaseEnvironment implements Environment {

    private final CondaPackageRegistry registry;

    protected BaseEnvironment() {
        this.registry = new CondaPackageRegistry(this);
    }

    public final CondaPackageRegistry getPackageRegistry() {
        return registry;
    }
}
