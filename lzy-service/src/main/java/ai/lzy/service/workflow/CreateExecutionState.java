package ai.lzy.service.workflow;

import ai.lzy.service.data.StorageType;
import ai.lzy.v1.common.LMS3;
import io.grpc.Status;

import java.util.UUID;

final class CreateExecutionState {
    private final String userId;
    private final String workflowName;
    private final String executionId;

    private StorageType storageType;
    private LMS3.S3Locator storageLocator;

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

    public void setStorageType(boolean internalStorage) {
        storageType = internalStorage ? StorageType.INTERNAL : StorageType.USER;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public LMS3.S3Locator getStorageLocator() {
        return storageLocator;
    }

    public void setStorageLocator(LMS3.S3Locator storageLocator) {
        this.storageLocator = storageLocator;
    }

    public boolean isInvalid() {
        return errorStatus != null;
    }

    public Status getErrorStatus() {
        return errorStatus;
    }

    public void onError(Status errorStatus, String description) {
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

        sb.append(printUserId()).append(", ");
        sb.append(printWorkflowName()).append(", ");
        sb.append(printExecutionId()).append(", ");

        if (storageType != null) {
            sb.append(printStorageType()).append(", ");
        }

        sb.append(" }");
        return sb.toString();
    }
}
