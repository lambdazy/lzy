package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public interface SlotSnapshotProvider {
    SlotSnapshot slotSnapshot(Slot slot);

    class Cached implements SlotSnapshotProvider {
        private final Map<String, SlotSnapshot> cache = new ConcurrentHashMap<>();
        private final Function<Slot, SlotSnapshot> creator;

        public Cached(Function<Slot, SlotSnapshot> creator) {
            this.creator = creator;
        }

        @Override
        public SlotSnapshot slotSnapshot(Slot slot) {
            return cache.computeIfAbsent(slot.name(), s -> creator.apply(slot));
        }
    }
}
