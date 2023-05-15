package ai.lzy.service.config;

record SemanticVersion(
    int major,
    int minor,
    int path
) {
    public boolean smallerThen(SemanticVersion other) {
        return major < other.major
            || major == other.major && minor < other.minor
            || major == other.major && minor == other.minor && path < other.path;
    }

    @Override
    public String toString() {
        return "%s.%s.%s".formatted(major, minor, path);
    }
}
