package ai.lzy.model.deprecated;

import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.slot.Slot;
import java.util.stream.Stream;

@Deprecated
public interface Context {
    Env env();

    Provisioning provisioning();

    Stream<SlotAssignment> assignments();

    class SlotAssignment {
        private final String task;
        private final Slot slot;
        private final String binding;

        public SlotAssignment(String task, Slot slot, String binding) {
            this.task = task;
            this.slot = slot;
            this.binding = binding;
        }

        public String task() {
            return task;
        }

        public Slot slot() {
            return slot;
        }

        public String binding() {
            return binding;
        }
    }

}
