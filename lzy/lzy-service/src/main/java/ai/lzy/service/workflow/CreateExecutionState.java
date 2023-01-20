package ai.lzy.service.workflow;

import ai.lzy.service.data.StorageType;
import ai.lzy.v1.common.LMST;
import io.grpc.Status;

import java.util.UUID;

final class CreateExecutionState {
    private final String userId;
    private final String workflowName;
    private final String executionId;
    private String sessionId;
    private String stdoutChannelId;
    private String stderrChannelId;
    private String portalId;

    private StorageType storageType;
    private LMST.StorageConfig storageConfig;

    private Status errorStatus;

    public CreateExecutionState(String userId, String workflowName) {
        this.userId = userId;
        this.workflowName = workflowName;
        this.executionId = workflowName + "_" + UUID.randomUUID();
    }

    public String getUserId() {
        return userId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStdoutChannelId() {
        return stdoutChannelId;
    }

    public void setStdoutChannelId(String stdoutChannelId) {
        this.stdoutChannelId = stdoutChannelId;
    }

    public String getStderrChannelId() {
        return stderrChannelId;
    }

    public void setStderrChannelId(String stderrChannelId) {
        this.stderrChannelId = stderrChannelId;
    }

    public String getPortalId() {
        return portalId;
    }

    public void setPortalId(String portalId) {
        this.portalId = portalId;
    }

    public void setStorageType(boolean internalStorage) {
        storageType = internalStorage ? StorageType.INTERNAL : StorageType.USER;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public LMST.StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public void setStorageConfig(LMST.StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
    }

    public boolean isInvalid() {
        return errorStatus != null;
    }

    public Status getErrorStatus() {
        return errorStatus;
    }

    public void fail(Status errorStatus, String description) {
        this.errorStatus = errorStatus.withDescription(description);
    }

    private String printUserId() {
        return "userId: " + userId;
    }

    private String printWorkflowName() {
        return "workflowName: " + workflowName;
    }

    private String printExecutionId() {
        return "executionId: " + executionId;
    }

    private String printStorageType() {
        return "storageType: " + storageType.name();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("{ ");

        sb.append(printUserId());
        sb.append(", ").append(printWorkflowName());
        sb.append(", ").append(printExecutionId());

        if (storageType != null) {
            sb.append(", ").append(printStorageType());
        }

        sb.append(" }");
        return sb.toString();
    }
}
