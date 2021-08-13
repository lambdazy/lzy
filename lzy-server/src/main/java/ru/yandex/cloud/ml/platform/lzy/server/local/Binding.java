package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Binding {
    private static Map<URI, Binding> bindings = new HashMap<>();

    public static synchronized Binding singleton(Slot slot, URI uri, Channel controlChannel) {
        return bindings.computeIfAbsent(uri, u -> new Binding(slot, uri, controlChannel));
    }

    public static synchronized void clearAll(URI servant) {
        final String servantStr = servant.toString();
        Set.copyOf(bindings.values()).stream()
            .map(Binding::uri).filter(u -> u.toString().startsWith(servantStr))
            .forEach(u -> bindings.remove(u));
    }

    private final URI uri;
    private Channel controlChannel;
    private final Slot slot;
    private boolean isInvalid = false;

    private Binding(Slot slot, URI uri, Channel controlChannel) {
        this.uri = uri;
        this.slot = slot;
        this.controlChannel = controlChannel;
    }

    public Channel control() {
        return controlChannel;
    }

    public URI uri() {
        return uri;
    }

    public Slot slot() {
        return slot;
    }

    public boolean isInvalid() {
        return isInvalid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Binding binding = (Binding) o;
        return uri.equals(binding.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    @Override
    public String toString() {
        return "(binding) " + uri;
    }

    public void invalidate() {
        isInvalid = true;
        controlChannel = null;
    }
}
