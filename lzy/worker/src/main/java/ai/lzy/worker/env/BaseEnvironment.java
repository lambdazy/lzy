package ai.lzy.worker.env;

public abstract class BaseEnvironment implements Environment {

    private final String id;
    private final CondaPackageRegistry registry;

    protected BaseEnvironment(String baseEnvId) {
        this.id = baseEnvId;
        this.registry = new CondaPackageRegistry();
    }

    final String baseEnvId() {
        return id;
    }

    final CondaPackageRegistry getPackageRegistry() {
        return registry;
    }

}
