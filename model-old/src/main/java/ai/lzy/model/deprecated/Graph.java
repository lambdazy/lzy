package ai.lzy.model.deprecated;

import ai.lzy.model.slot.Slot;

@Deprecated
public interface Graph extends Zygote {
    Slot[] allSockets();

    interface Builder {
        Builder append(Zygote op);

        Builder link(Slot from, Slot to); // ChannelSpec ch);

        Graph build();
    }
}
