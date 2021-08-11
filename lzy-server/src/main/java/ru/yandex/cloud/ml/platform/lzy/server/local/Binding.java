package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.net.URI;
import java.util.Objects;

public class Binding {
    private final URI uri;
    private final Channel controlChannel;
    private final Slot slot;
    private boolean isInvalid = false;

    public Binding(Task task, Slot slot) {
        this(slot, task.servant().resolve(slot.name()), task.servantChannel());
    }

    public Binding(Slot slot, URI uri, Channel controlChannel) {
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
        return slot.equals(binding.slot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slot);
    }

    public void invalidate() {
        isInvalid = true;
    }
}
