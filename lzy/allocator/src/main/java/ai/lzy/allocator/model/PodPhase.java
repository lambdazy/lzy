package ai.lzy.allocator.model;

public enum PodPhase {
    PENDING("Pending"),
    RUNNING("Running"),
    SUCCEEDED("Succeeded"),
    FAILED("Failed"),
    UNKNOWN("Unknown"),
    ;

    private final String phase;

    PodPhase(String phase) {
        this.phase = phase;
    }

    public static PodPhase fromString(String phase) {
        return switch (phase) {
            case "Pending" -> PENDING;
            case "Running" -> RUNNING;
            case "Succeeded" -> SUCCEEDED;
            case "Failed" -> FAILED;
            case "Unknown" -> UNKNOWN;
            default -> throw new IllegalStateException("Unknown pod phase: " + phase);
        };
    }
}
