package ai.lzy.allocator.vmpool;

public enum ProvisioningValues {
    ICE_LAKE("Intel Ice Lake"),
    CASCADE_LAKE("Intel Cascade Lake"),
    BROADWELL("Intel Broadwell"),
    AMD_EPYC("AMD EPYC"),
    NO_GPU("<none>"),
    V100("V100"),
    A100("A100"),
    T4("T4")

    ;
    private final String value;

    ProvisioningValues(String name) {
        this.value = name;
    }

    public String value() {
        return value;
    }
}
