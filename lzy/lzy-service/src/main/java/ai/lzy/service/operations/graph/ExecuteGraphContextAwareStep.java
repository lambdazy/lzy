package ai.lzy.service.operations.graph;

import ai.lzy.service.dao.DataFlowGraph;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionContextAwareStep;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.workflow.LWF;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static ai.lzy.model.db.DbHelper.withRetries;

public abstract class ExecuteGraphContextAwareStep extends ExecutionContextAwareStep {
    private final ExecuteGraphState state;

    public ExecuteGraphContextAwareStep(ExecutionStepContext stepCtx, ExecuteGraphState state) {
        super(stepCtx);
        this.state = state;
    }

    protected ExecuteGraphState state() {
        return state;
    }

    protected void saveState() throws Exception {
        withRetries(log(), () -> execOpsDao().putState(opId(), state(), null));
    }

    protected LWF.Graph request() {
        return state.request;
    }

    @Nullable
    protected List<LWF.Operation> operationsToExecute() {
        return state.operationsToExecute;
    }

    protected void setOperationsToExecute(List<LWF.Operation> ops) {
        state.operationsToExecute = ops;
    }

    @Nullable
    protected String vmPoolZone() {
        return state.vmPoolZone;
    }

    protected void setVmPoolZone(String vmPoolZone) {
        state.vmPoolZone = vmPoolZone;
    }

    @Nullable
    protected DataFlowGraph dataFlowGraph() {
        return state.dataFlowGraph;
    }

    protected void setDataFlowGraph(DataFlowGraph dataFlowGraph) {
        state.dataFlowGraph = dataFlowGraph;
    }

    @Nullable
    protected Map<String, String> volatileChannelsNames() {
        return state.portalOutputSlotUri2channelName;
    }

    protected void setVolatileChannelsNames(Map<String, String> channelName2slotUri) {
        state.portalOutputSlotUri2channelName = channelName2slotUri;
    }

    @Nullable
    protected Map<String, String> channels() {
        return state.slotUri2channelId;
    }

    protected void setChannels(Map<String, String> slotUri2channelId) {
        state.slotUri2channelId = slotUri2channelId;
    }

    @Nullable
    protected List<String> portalInputSlotsNames() {
        return state.portalInputSlotsNames;
    }

    protected void setPortalInputSlotsNames(List<String> slotsNames) {
        state.portalInputSlotsNames = slotsNames;
    }

    @Nullable
    protected Map<String, String> portalSlotsNames() {
        return state.portalSlotUri2slotName;
    }

    protected void setPortalSlotsNames(Map<String, String> slotsNames) {
        state.portalSlotUri2slotName = slotsNames;
    }

    @Nullable
    protected List<GraphExecutor.TaskDesc> tasks() {
        return state.tasks;
    }

    protected void setTasks(List<GraphExecutor.TaskDesc> tasks) {
        state.tasks = tasks;
    }

    @Nullable
    protected String graphId() {
        return state.graphId;
    }

    protected void setGraphId(String graphId) {
        state.graphId = graphId;
    }
}
