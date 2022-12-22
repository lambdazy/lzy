package ai.lzy.service.graph;

import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.Status;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@JsonSerialize
@JsonDeserialize
@NoArgsConstructor
public final class GraphExecutionState {
    private String opId;
    private String idempotencyKey;

    private String userId;
    private String executionId;
    private String workflowName;

    private String graphId;
    private String parentGraphId;

    private String zone;
    private List<LWF.Operation> operations;
    private List<LWF.DataDescription> descriptions;

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

    public String getOpId() {
        return opId;
    }

    public void setOpId(String opId) {
        this.opId = opId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(String graphId) {
        this.graphId = graphId;
    }

    public String getParentGraphId() {
        return parentGraphId;
    }

    public void setParentGraphId(String parentGraphId) {
        this.parentGraphId = parentGraphId;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public List<LWF.Operation> getOperations() {
        return operations;
    }

    public void setOperations(List<LWF.Operation> operations) {
        this.operations = operations;
    }

    public List<LWF.DataDescription> getDescriptions() {
        return descriptions;
    }

    public void setDescriptions(List<LWF.DataDescription> descriptions) {
        this.descriptions = descriptions;
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

    public List<String> getPortalInputSlots() {
        return portalInputSlots;
    }

    public void setPortalInputSlots(List<String> portalInputSlots) {
        this.portalInputSlots = portalInputSlots;
    }

    public DataFlowGraph getDataFlowGraph() {
        return dataFlowGraph;
    }

    public void setDataFlowGraph(DataFlowGraph dataFlowGraph) {
        this.dataFlowGraph = dataFlowGraph;
    }

    public Status getErrorStatus() {
        return errorStatus;
    }

    public void setErrorStatus(Status errorStatus) {
        this.errorStatus = errorStatus;
    }

    @JsonIgnore
    public String getOrGenerateIdempotencyKey() {
        return idempotencyKey = (idempotencyKey != null) ? idempotencyKey : UUID.randomUUID().toString();
    }

    @JsonIgnore
    public boolean isInvalid() {
        return errorStatus != null;
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
