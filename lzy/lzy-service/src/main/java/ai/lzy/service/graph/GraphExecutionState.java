package ai.lzy.service.graph;

import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.Status;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@JsonSerialize
@JsonDeserialize
@NoArgsConstructor
@Getter
@Setter
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

    public GraphExecutionState(String userId, String workflowName, String executionId, String opId, LWF.Graph graph) {
        this.userId = userId;
        this.workflowName = workflowName;
        this.executionId = executionId;
        this.opId = opId;
        this.parentGraphId = graph.getParentGraphId();
        this.zone = graph.getZone();
        this.descriptions = graph.getDataDescriptionsList();
        this.operations = graph.getOperationsList();
    }

    @JsonIgnore
    public boolean isInvalid() {
        return errorStatus != null;
    }

    public void fail(Status errorStatus, String description) {
        this.errorStatus = errorStatus.withDescription(description);
    }

    private String printOperationId() {
        return "operationId: " + opId;
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

    private String printIdempotencyKey() {
        return "idempotencyKey: " + idempotencyKey;
    }

    private String printGraphId() {
        return "graphId: " + graphId;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("{ ");

        sb.append(printExecutionId());
        sb.append(", ").append(printOperationId());
        if (workflowName != null) {
            sb.append(", ").append(printWorkflowName());
        }
        if (zone != null) {
            sb.append(", ").append(printZoneName());
        }
        if (dataFlowGraph != null) {
            sb.append(", ").append(printDataflowGraph());
        }
        if (idempotencyKey != null) {
            sb.append(", ").append(printIdempotencyKey());
        }
        if (graphId != null) {
            sb.append(", ").append(printGraphId());
        }

        sb.append(" }");
        return sb.toString();
    }
}
