package ru.yandex.cloud.ml.platform.lzy.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public class ArgumentsSlot extends LzySlotBase {
    private final String arguments;

    public ArgumentsSlot(Slot definition, String arguments) {
        super(definition);
        this.arguments = arguments;
    }

    public String getArguments() {
        return arguments;
    }
}
