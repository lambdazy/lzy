package ai.lzy.service.dao;

import jakarta.annotation.Nullable;

public class DeleteAllocatorSessionState {
    private final String sessionId;
    @Nullable
    private String deleteOpId;

    public DeleteAllocatorSessionState(String sessionId, @Nullable String deleteOpId) {
        this.sessionId = sessionId;
        this.deleteOpId = deleteOpId;
    }

    public String sessionId() {
        return sessionId;
    }

    @Nullable
    public String deleteOpId() {
        return deleteOpId;
    }

    public void setDeleteOpId(@Nullable String deleteOpId) {
        this.deleteOpId = deleteOpId;
    }
}
