package ai.lzy.model.deprecated;

import ai.lzy.model.slot.Slot;
import java.util.stream.Stream;
import javax.annotation.Nullable;

@Deprecated
public interface Execution {
    Zygote operation();

    Stream<Communication> incoming();

    Stream<Communication> outgoing();

    ReproducibilityLevel rl();

    enum ReproducibilityLevel {
        ByteLevel,
        StatLevel,
        SratLevel
    }

    interface Communication {
        Slot socket();

        @Nullable
        byte[] content();

        long hash();

        long duration();
    }
}
