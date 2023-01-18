package ai.lzy.allocator.disk;

import java.time.Instant;

public record DiskOperation(
    String opId,
    String descr,
    Instant startedAt,
    Instant deadline,
    String ownerInstanceId,
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
        return new DiskOperation(opId, descr, startedAt, deadline, ownerInstanceId, diskOpType, state, deferredAction);
    }
}
