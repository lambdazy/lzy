package ai.lzy.service.workflow;

import ai.lzy.v1.common.LMST;
import io.grpc.Status;

import java.util.UUID;

final class CreateExecutionState {
    private final String userId;
    private final String workflowName;
    private final String executionId;
    private final LMST.StorageConfig storageConfig;
    private final String storageName;

    private String sessionId;
    private String stdoutChannelId;
    private String stderrChannelId;
    private String portalId;
    private Status errorStatus;

    public CreateExecutionState(String userId, String workflowName, String storageName,
                                LMST.StorageConfig storageConfig)
    {
        this.userId = userId;
        this.workflowName = workflowName;
        this.storageConfig = storageConfig;
        this.storageName = storageName;
        this.executionId = workflowName + "_" + UUID.randomUUID();
    }

    public String getStorageName() {
        return storageName;
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

    public LMST.StorageConfig getStorageConfig() {
        return storageConfig;
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

    private String printStorageName() {
        return "storageName: " + storageName;
    }

    @Override
    public String toString() {
        return "{ " +
            printUserId() +
            ", " + printWorkflowName() +
            ", " + printExecutionId() +
            ", " + printStorageName() +
            " }";
    }
}
