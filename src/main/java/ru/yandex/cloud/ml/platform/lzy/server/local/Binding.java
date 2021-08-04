package ru.yandex.cloud.ml.platform.lzy.server.local;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.util.Objects;

public class Binding {
    private final Task task;
    private final Slot slot;

    Binding(Task task, Slot slot) {
        this.task = task;
        this.slot = slot;
    }

    public Task task() {
        return task;
    }

    public Slot slot() {
        return slot;
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
        return task.equals(binding.task) &&
            slot.equals(binding.slot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task, slot);
    }
}
