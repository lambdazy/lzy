package ai.lzy.allocator.vmpool;

public enum GpuTypes {
    NO_GPU("NO_GPU"),
    V100("V100"),
    A100("A100"),
    T4("T4");

    private final String value;

    GpuTypes(String name) {
        this.value = name;
    }

    public String value() {
        return value;
    }
}
