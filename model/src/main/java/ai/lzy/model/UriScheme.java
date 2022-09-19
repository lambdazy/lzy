package ai.lzy.model;

import java.net.URI;
import javax.annotation.Nonnull;

public enum UriScheme {
    // services
    LzyFs("fs"),
    LzyServant("servant"),
    LzyTerminal("terminal"),
    LzyKharon("kharon"),

    // slots
    SlotS3("s3"),
    SlotAzure("azure"),
    Snapshot("snapshot");

    private final String scheme;

    UriScheme(@Nonnull  String scheme) {
        this.scheme = scheme;
    }

    public String scheme() {
        return scheme;
    }

    public boolean match(URI uri) {
        return scheme.equals(uri.getScheme());
    }
}
