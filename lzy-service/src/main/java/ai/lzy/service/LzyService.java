package ai.lzy.service;

import ai.lzy.service.gc.GarbageCollector;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;

import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    public static final String APP = "LzyService";

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;
    private final GarbageCollector gc;

    public LzyService(WorkflowService workflowService,
                      GraphExecutionService graphExecutionService,
                      GarbageCollector gc) {
        this.workflowService = workflowService;
        this.graphExecutionService = graphExecutionService;
        this.gc = gc;
    }

    @Override
    public void createWorkflow(CreateWorkflowRequest request, StreamObserver<CreateWorkflowResponse> responseObserver) {
        workflowService.createWorkflow(request, responseObserver);
    }

    @Override
    public void attachWorkflow(AttachWorkflowRequest request, StreamObserver<AttachWorkflowResponse> responseObserver) {
        workflowService.attachWorkflow(request, responseObserver);
    }

    @Override
    public void finishWorkflow(FinishWorkflowRequest request, StreamObserver<FinishWorkflowResponse> responseObserver) {
        workflowService.finishWorkflow(request, responseObserver);
    }

    @Override
    public void executeGraph(ExecuteGraphRequest request, StreamObserver<ExecuteGraphResponse> responseObserver) {
        graphExecutionService.executeGraph(request, responseObserver);
    }

    @Override
    public void graphStatus(GraphStatusRequest request, StreamObserver<GraphStatusResponse> responseObserver) {
        graphExecutionService.graphStatus(request, responseObserver);
    }

    @Override
    public void stopGraph(StopGraphRequest request, StreamObserver<StopGraphResponse> responseObserver) {
        graphExecutionService.stopGraph(request, responseObserver);
    }

    @Override
    public void readStdSlots(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> responseObserver) {
        workflowService.readStdSlots(request, responseObserver);
    }

    @Override
    public void getAvailablePools(GetAvailablePoolsRequest request,
                                  StreamObserver<GetAvailablePoolsResponse> responseObserver)
    {
        workflowService.getAvailablePools(request, responseObserver);
    }
}
