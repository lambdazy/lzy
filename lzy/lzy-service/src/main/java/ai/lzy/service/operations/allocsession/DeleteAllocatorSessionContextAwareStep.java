package ai.lzy.service.operations.allocsession;

import ai.lzy.service.dao.DeleteAllocatorSessionState;
import ai.lzy.service.operations.ExecutionContextAwareStep;
import ai.lzy.service.operations.ExecutionStepContext;
import jakarta.annotation.Nullable;

public class DeleteAllocatorSessionContextAwareStep extends ExecutionContextAwareStep {
    private final DeleteAllocatorSessionState state;

    public DeleteAllocatorSessionContextAwareStep(ExecutionStepContext stepCtx, DeleteAllocatorSessionState state) {
        super(stepCtx);
        this.state = state;
    }

    public String sessionId() {
        return state.sessionId();
    }

    @Nullable
    public String deleteSessionOpId() {
        return state.deleteOpId();
    }

    public void setDeleteSessionOpId(String deleteSessionOpId) {
        state.setDeleteOpId(deleteSessionOpId);
    }
}
