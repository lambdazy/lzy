package ai.lzy.service.graph;

import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;

import java.util.List;

final class GraphExecutionState {
    private final String executionId;

    private String workflowName;
    private String zoneName;

    private List<LWF.DataDescription> descriptions;
    private List<LWF.Operation> operations;
    private List<TaskDesc> tasks;
    private List<ChannelDesc> channels;

    private DataFlowGraph dataFlowGraph;

    private Status errorStatus;

    public GraphExecutionState(String executionId) {
        this.executionId = executionId;
    }

    public List<LWF.DataDescription> getDescriptions() {
        return descriptions;
    }

    public void setDescriptions(List<LWF.DataDescription> descriptions) {
        this.descriptions = descriptions;
    }

    public String getExecutionId() {
        return executionId;
    }

    public List<LWF.Operation> getOperations() {
        return operations;
    }

    public void setOperations(List<LWF.Operation> operations) {
        this.operations = operations;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    public DataFlowGraph getDataFlowGraph() {
        return dataFlowGraph;
    }

    public void setDataFlowGraph(DataFlowGraph dataFlowGraph) {
        this.dataFlowGraph = dataFlowGraph;
    }

    public List<TaskDesc> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDesc> tasks) {
        this.tasks = tasks;
    }

    public List<ChannelDesc> getChannels() {
        return channels;
    }

    public void setChannels(List<ChannelDesc> channels) {
        this.channels = channels;
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

    private String printExecutionId() {
        return "executionId: " + executionId;
    }

    private String printWorkflowName() {
        return "workflowName: " + workflowName;
    }

    private String printZoneName() {
        return "zoneName: " + zoneName;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("{ ");

        sb.append(printExecutionId());
        if (workflowName != null) {
            sb.append(", ").append(printWorkflowName());
        }
        if (zoneName != null) {
            sb.append(", ").append(printZoneName());
        }

        sb.append(" }");
        return sb.toString();
    }
}
