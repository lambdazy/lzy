package ai.lzy.allocator.vmpool;

public enum CpuTypes {
    CASCADE_LAKE("Intel Cascade Lake"),
    BROADWELL("Intel Broadwell"),
    AMD_EPYC("AMD EPYC"),
    ICE_LAKE("Intel Ice Lake");

    private final String value;

    CpuTypes(String name) {
        this.value = name;
    }

    public String value() {
        return value;
    }
}
