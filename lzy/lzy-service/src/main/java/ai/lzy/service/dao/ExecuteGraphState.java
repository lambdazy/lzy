package ai.lzy.service.dao;

import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import ai.lzy.v1.workflow.LWF;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public Map<String, String> portalOutputSlotUri2channelName;
    @Nullable
    public Map<String, String> slotUri2channelId;
    @Nullable
    public Map<String, String> portalSlotUri2slotName;
    @Nullable
    public List<TaskDesc> tasks;
    @Nullable
    public String graphId;

    public ExecuteGraphState(LWF.Graph request) {
        this.request = request;
    }
}
