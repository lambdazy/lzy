package ai.lzy.service;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.whiteboard.WhiteboardService;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

    public static final String APP = "LzyService";

    private final ManagedChannel allocatorServiceChannel;
    private final ManagedChannel operationServiceChannel;
    private final ManagedChannel storageServiceChannel;
    private final ManagedChannel storageOperationServiceChannel;
    private final ManagedChannel channelManagerChannel;
    private final ManagedChannel iamChannel;
    private final ManagedChannel whiteboardChannel;
    private final ManagedChannel graphExecutorChannel;

    private final WorkflowService workflowService;
    private final WhiteboardService whiteboardService;
    private final GraphExecutionService graphExecutionService;

    public LzyService(LzyServiceConfig config, LzyServiceStorage storage,
                      WorkflowDao workflowDao, ExecutionDao executionDao, GraphDao graphDao)
    {
        String channelManagerAddress = config.getChannelManagerAddress();

        String iamAddress = config.getIam().getAddress();
        var creds = config.getIam().createRenewableToken();

        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        var allocatorAddress = HostAndPort.fromString(config.getAllocatorAddress());

        allocatorServiceChannel = newGrpcChannel(allocatorAddress, AllocatorGrpc.SERVICE_NAME);
        var allocatorClient = newBlockingClient(
            AllocatorGrpc.newBlockingStub(allocatorServiceChannel), APP, () -> creds.get().token());
        var vmPoolClient = newBlockingClient(
            VmPoolServiceGrpc.newBlockingStub(allocatorServiceChannel), APP, () -> creds.get().token());

        operationServiceChannel = newGrpcChannel(allocatorAddress, LongRunningServiceGrpc.SERVICE_NAME);
        var operationServiceClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(operationServiceChannel), APP, () -> creds.get().token());

        var storageServiceAddress = config.getStorage().getAddress();

        storageServiceChannel = newGrpcChannel(storageServiceAddress, LzyStorageServiceGrpc.SERVICE_NAME);
        var storageServiceClient = newBlockingClient(
            LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel), APP, () -> creds.get().token());

        storageOperationServiceChannel = newGrpcChannel(storageServiceAddress, LongRunningServiceGrpc.SERVICE_NAME);
        var storageOperationServiceClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(storageOperationServiceChannel), APP, () -> creds.get().token());

        channelManagerChannel = newGrpcChannel(channelManagerAddress, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        var channelManagerClient = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP, () -> creds.get().token());

        iamChannel = newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var subjectClient = new SubjectServiceGrpcClient(APP, iamChannel, creds::get);
        var abClient = new AccessBindingServiceGrpcClient(APP, iamChannel, creds::get);

        whiteboardChannel = newGrpcChannel(config.getWhiteboardAddress(), LzyWhiteboardPrivateServiceGrpc.SERVICE_NAME);
        var whiteboardClient = newBlockingClient(
            LzyWhiteboardPrivateServiceGrpc.newBlockingStub(whiteboardChannel), APP, () -> creds.get().token());

        graphExecutorChannel = newGrpcChannel(config.getGraphExecutorAddress(), GraphExecutorGrpc.SERVICE_NAME);
        var graphExecutorClient = newBlockingClient(
            GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP, () -> creds.get().token());

        workflowService = new WorkflowService(config, channelManagerClient, allocatorClient,
            operationServiceClient, subjectClient, abClient, storageServiceChannel, storageOperationServiceClient,
            storage, workflowDao);
        whiteboardService = new WhiteboardService(whiteboardClient);
        graphExecutionService = new GraphExecutionService(creds, workflowDao, graphDao, executionDao,
            vmPoolClient, graphExecutorClient, channelManagerClient);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        GrpcChannels.awaitTermination(allocatorServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(operationServiceChannel, Duration.ofSeconds(10), getClass());
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
}
