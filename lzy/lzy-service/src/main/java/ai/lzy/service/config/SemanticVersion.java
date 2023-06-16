package ai.lzy.service.config;

public record SemanticVersion(
    int major,
    int minor,
    int patch
) {
    public boolean lessThen(SemanticVersion other) {
        return major < other.major
            || major == other.major && minor < other.minor
            || major == other.major && minor == other.minor && patch < other.patch;
    }

    @Override
    public String toString() {
        return "%s.%s.%s".formatted(major, minor, patch);
    }
}
