package ru.yandex.cloud.ml.platform.lzy.model;

import java.util.List;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

public interface Context {
    Env env();
    Provisioning provisioning();
    SnapshotMeta meta();
    List<SlotAssignment> assignments();

    class SlotAssignment{
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

    class ContextImpl implements Context{

        private final Env env;
        private final Provisioning provisioning;
        private final SnapshotMeta meta;
        private final List<SlotAssignment> assignments;

        public ContextImpl(Env env, Provisioning provisioning,
            SnapshotMeta meta,
            List<SlotAssignment> assignments) {
            this.env = env;
            this.provisioning = provisioning;
            this.meta = meta;
            this.assignments = assignments;
        }

        @Override
        public Env env() {
            return env;
        }

        @Override
        public Provisioning provisioning() {
            return provisioning;
        }

        @Override
        public SnapshotMeta meta() {
            return meta;
        }

        @Override
        public List<SlotAssignment> assignments() {
            return assignments;
        }
    }
}
