package ai.lzy.service.graph;

import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;

import java.util.List;
import java.util.UUID;

public final class GraphExecutionState {
    private final String executionId;

    private final String opId;
    private final String parentGraphId;

    private final String userId;

    private String zone;
    private final List<LWF.DataDescription> descriptions;
    private final List<LWF.Operation> operations;

    private String workflowName;

    private String idempotencyKey;
    private String graphId;

    private List<TaskDesc> tasks;
    private List<ChannelDesc> channels;
    private List<String> portalInputSlots;

    private DataFlowGraph dataFlowGraph;

    private Status errorStatus;

    public GraphExecutionState(String executionId, String opId, String parentGraphId, String userId,
                               String zone, List<LWF.DataDescription> descriptions,
                               List<LWF.Operation> operations)
    {
        this.executionId = executionId;
        this.opId = opId;
        this.parentGraphId = parentGraphId;
        this.userId = userId;
        this.zone = zone;
        this.descriptions = descriptions;
        this.operations = operations;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getParentGraphId() {
        return parentGraphId;
    }

    public String getOpId() {
        return opId;
    }

    public String getOrGenerateIdempotencyKey() {
        return idempotencyKey = (idempotencyKey != null) ? idempotencyKey : UUID.randomUUID().toString();
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(String graphId) {
        this.graphId = graphId;
    }

    public List<LWF.DataDescription> getDescriptions() {
        return descriptions;
    }

    public String getExecutionId() {
        return executionId;
    }

    public List<LWF.Operation> getOperations() {
        return operations;
    }

    public String getUserId() {
        return userId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getZone() {
        return zone;
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

    public void setPortalInputSlots(List<String> portalInputSlots) {
        this.portalInputSlots = portalInputSlots;
    }

    public List<String> getPortalInputSlots() {
        return portalInputSlots;
    }

    public void fail(Status errorStatus, String description) {
        this.errorStatus = errorStatus.withDescription(description);
    }

    private String printExecutionId() {
        return "executionId: " + executionId;
    }

    private String printWorkflowName() {
        return "workflowName: " + workflowName;
    }

    private String printZoneName() {
        return "zoneName: " + zone;
    }

    private String printDataflowGraph() {
        return "dataflowGraph: " + dataFlowGraph.toString();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("{ ");

        sb.append(printExecutionId());
        if (workflowName != null) {
            sb.append(", ").append(printWorkflowName());
        }
        if (zone != null) {
            sb.append(", ").append(printZoneName());
        }
        if (dataFlowGraph != null) {
            sb.append(", ").append(printDataflowGraph());
        }

        sb.append(" }");
        return sb.toString();
    }
}
