package ai.lzy.worker.env;

public abstract class BaseEnvironment implements Environment {

    private final CondaPackageRegistry registry;

    protected BaseEnvironment() {
        this.registry = new CondaPackageRegistry();
    }

    final CondaPackageRegistry getPackageRegistry() {
        return registry;
    }

}
