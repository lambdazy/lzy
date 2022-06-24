package ai.lzy.model.graph;

import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.Slot;
import ai.lzy.model.Zygote;

public interface Graph extends Zygote {
    Slot[] allSockets();

    interface Builder {
        Builder append(Zygote op);

        Builder link(Slot from, Slot to, ChannelSpec ch);

        Graph build();
    }
}
