package ai.lzy.allocator.disk;

import java.time.Instant;

public record DiskOperation(
    String opId,
    Instant startedAt,
    Instant deadline,
    Type diskOpType,
    String state,
    Runnable deferredAction
) {
    public enum Type {
        CREATE,
        CLONE,
        DELETE
    }

    public DiskOperation withDeferredAction(Runnable deferredAction) {
        return new DiskOperation(opId, startedAt, deadline, diskOpType, state, deferredAction);
    }
}
