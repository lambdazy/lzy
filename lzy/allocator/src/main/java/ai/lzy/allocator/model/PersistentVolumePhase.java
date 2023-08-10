package ai.lzy.allocator.model;

public enum PersistentVolumePhase {
    // https://kubernetes.io/docs/concepts/storage/persistent-volumes/#phase

    AVAILABLE("Available"),
    BOUND("Bound"),
    RELEASED("Released"),
    FAILED("Failed")
    ;

    private final String phase;

    PersistentVolumePhase(String phase) {
        this.phase = phase;
    }

    public static PersistentVolumePhase fromString(String phase) {
        return switch (phase) {
            case "Available" -> AVAILABLE;
            case "Bound" -> BOUND;
            case "Released" -> RELEASED;
            case "Failed" -> FAILED;
            default -> throw new IllegalStateException("Unknown pv phase: " + phase);
        };
    }

    public String getPhase() {
        return phase;
    }
}
