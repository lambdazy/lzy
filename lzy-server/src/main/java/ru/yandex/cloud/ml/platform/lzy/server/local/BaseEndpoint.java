package ru.yandex.cloud.ml.platform.lzy.server.local;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

public abstract class BaseEndpoint implements Endpoint {
    private final URI uri;
    private final Slot slot;
    private final UUID sessionId;
    private boolean invalid = false;

    public BaseEndpoint(Slot slot, URI uri, UUID sessionId) {
        this.uri = uri;
        this.slot = slot;
        this.sessionId = sessionId;
    }

    public URI uri() {
        return uri;
    }

    public Slot slot() {
        return slot;
    }

    @Override
    public UUID sessionId() {
        return sessionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BaseEndpoint endpoint = (BaseEndpoint) o;
        return uri.equals(endpoint.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    @Override
    public String toString() {
        return "(endpoint) {" + uri + "}";
    }

    @Override
    public boolean isInvalid() {
        return invalid;
    }

    protected void invalidate() {
        invalid = true;
    }
}
