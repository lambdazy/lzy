package ai.lzy.model;

import jakarta.annotation.Nonnull;

import java.net.URI;

public enum UriScheme {
    // services
    LzyFs("fs"),
    LzyWorker("worker"),
    LzyTerminal("terminal"),
    LzyKharon("kharon"),

    // slots
    SlotS3("s3"),
    SlotAzure("azure"),
    Snapshot("snapshot");

    private final String scheme;

    UriScheme(@Nonnull String scheme) {
        this.scheme = scheme;
    }

    public String scheme() {
        return scheme;
    }

    public boolean match(URI uri) {
        return scheme.equals(uri.getScheme());
    }
}
