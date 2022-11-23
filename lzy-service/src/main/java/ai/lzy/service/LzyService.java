package ai.lzy.service;

import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.whiteboard.WhiteboardService;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String APP = "LzyService";

    private final ManagedChannel allocatorServiceChannel;
    private final ManagedChannel storageServiceChannel;
    private final ManagedChannel channelManagerChannel;
    private final ManagedChannel iamChannel;
    private final ManagedChannel whiteboardChannel;
    private final ManagedChannel graphExecutorChannel;

    private final WorkflowService workflowService;
    private final WhiteboardService whiteboardService;
    private final GraphExecutionService graphExecutionService;

    public LzyService(WorkflowService workflowService, WhiteboardService whiteboardService,
                      GraphExecutionService graphExecutionService,
                      @Named("AllocatorServiceChannel") ManagedChannel allocatorChannel,
                      @Named("StorageServiceChannel") ManagedChannel storageChannel,
                      @Named("ChannelManagerServiceChannel") ManagedChannel channelManagerChannel,
                      @Named("IamServiceChannel") ManagedChannel iamChannel,
                      @Named("WhiteboardServiceChannel") ManagedChannel whiteboardChannel,
                      @Named("GraphExecutorServiceChannel") ManagedChannel graphExecutorChannel)
    {
        this.allocatorServiceChannel = allocatorChannel;
        this.storageServiceChannel = storageChannel;
        this.channelManagerChannel = channelManagerChannel;
        this.iamChannel = iamChannel;
        this.whiteboardChannel = whiteboardChannel;
        this.graphExecutorChannel = graphExecutorChannel;

        this.workflowService = workflowService;
        this.whiteboardService = whiteboardService;
        this.graphExecutionService = graphExecutionService;
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        GrpcChannels.awaitTermination(allocatorServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(storageServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(channelManagerChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(iamChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(whiteboardChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(graphExecutorChannel, Duration.ofSeconds(10), getClass());
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
    public void createWhiteboard(CreateWhiteboardRequest request,
                                 StreamObserver<CreateWhiteboardResponse> responseObserver)
    {
        whiteboardService.createWhiteboard(request, responseObserver);
    }

    @Override
    public void linkWhiteboard(LinkWhiteboardRequest request, StreamObserver<LinkWhiteboardResponse> responseObserver) {
        whiteboardService.linkWhiteboard(request, responseObserver);
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
