package ai.lzy.allocator.volume;

public record VolumeMount(
    String name,
    String path,
    boolean readOnly,
    MountPropagation mountPropagation
) {
    public enum MountPropagation {
        NONE("None"),
        HOST_TO_CONTAINER("HostToContainer"),
        BIDIRECTIONAL("Bidirectional");

        private final String repr;

        MountPropagation(String repr) {
            this.repr = repr;
        }

        public String asString() {
            return repr;
        }
    }
}
