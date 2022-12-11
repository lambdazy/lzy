package ai.lzy.fs.slots;

import ai.lzy.model.slot.SlotInstance;

public class ArgumentsSlot extends LzySlotBase {
    private final String arguments;

    public ArgumentsSlot(SlotInstance instance, String arguments) {
        super(instance);
        this.arguments = arguments;
    }

    public String getArguments() {
        return arguments;
    }
}
