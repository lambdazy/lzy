package ai.lzy.fs.snapshot;

import ai.lzy.model.SlotInstance;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import ai.lzy.model.Slot;

public interface SlotSnapshotProvider {
    SlotSnapshot slotSnapshot(SlotInstance slot);

    class Cached implements SlotSnapshotProvider {
        private final Map<String, SlotSnapshot> cache = new ConcurrentHashMap<>();
        private final Function<SlotInstance, SlotSnapshot> creator;

        public Cached(Function<SlotInstance, SlotSnapshot> creator) {
            this.creator = creator;
        }

        @Override
        public SlotSnapshot slotSnapshot(SlotInstance slot) {
            return cache.computeIfAbsent(slot.name(), s -> creator.apply(slot));
        }
    }
}
