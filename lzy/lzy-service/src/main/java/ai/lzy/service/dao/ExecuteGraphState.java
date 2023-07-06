package ai.lzy.service.dao;

import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@JsonSerialize
@JsonDeserialize
@NoArgsConstructor
public final class ExecuteGraphState {
    // from ExecuteGraphRequest
    public LWF.Graph request;
    @Nullable
    public List<LWF.Operation> operationsToExecute;
    @Nullable
    public String vmPoolZone;
    @Nullable
    public DataFlowGraph dataFlowGraph;
    @Nullable
    public Map<String, String> slotUri2channelId;
    @Nullable
    public List<TaskDesc> tasks;
    @Nullable
    public String graphId;

    public ExecuteGraphState(LWF.Graph request) {
        this.request = request;
    }

    @Override
    public String toString() {
        // todo: make string more clarify
        return "ExecuteGraphState{" +
            "request=" + request +
            ", operationsToExecute=" + operationsToExecute +
            ", vmPoolZone='" + vmPoolZone + '\'' +
            ", dataFlowGraph=" + dataFlowGraph +
            ", slotUri2channelId=" + slotUri2channelId +
            ", tasks=" + tasks +
            ", graphId='" + graphId + '\'' +
            '}';
    }
}
