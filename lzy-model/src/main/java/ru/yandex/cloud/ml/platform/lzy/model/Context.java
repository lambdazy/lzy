package ru.yandex.cloud.ml.platform.lzy.model;

import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

public interface Context {
    Env env();

    Provisioning provisioning();

    SnapshotMeta meta();

    Stream<SlotAssignment> assignments();

    class SlotAssignment {
        private final Slot slot;
        private final String binding;

        public SlotAssignment(Slot slot, String binding) {
            this.slot = slot;
            this.binding = binding;
        }

        public Slot slot() {
            return slot;
        }

        public String binding() {
            return binding;
        }
    }

}
