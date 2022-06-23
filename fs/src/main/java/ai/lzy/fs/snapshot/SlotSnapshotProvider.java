package ai.lzy.fs.snapshot;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import ai.lzy.model.Slot;

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
