package ai.lzy.service;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.grpc.GrpcChannels;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
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
    private final ManagedChannel channelManagerChannel;
    private final ManagedChannel iamChannel;
    private final ManagedChannel graphExecutorChannel;

    private final WorkflowService workflowService;
    private final GraphExecutionService graphExecutionService;

    public LzyService(LzyServiceConfig config, LzyServiceStorage storage,
                      WorkflowDao workflowDao, ExecutionDao executionDao)
    {
        String channelManagerAddress = config.getChannelManagerAddress();

        String iamAddress = config.getIam().getAddress();
        var internalUserCredentials = config.getIam().createRenewableToken();

        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        var allocatorAddress = HostAndPort.fromString(config.getAllocatorAddress());

        allocatorServiceChannel = newGrpcChannel(allocatorAddress, AllocatorGrpc.SERVICE_NAME);
        AllocatorGrpc.AllocatorBlockingStub allocatorClient =
            newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorServiceChannel), APP,
                () -> internalUserCredentials.get().token());
        VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient =
            newBlockingClient(VmPoolServiceGrpc.newBlockingStub(allocatorServiceChannel), APP,
                () -> internalUserCredentials.get().token());

        operationServiceChannel = newGrpcChannel(allocatorAddress, LongRunningServiceGrpc.SERVICE_NAME);
        LongRunningServiceGrpc.LongRunningServiceBlockingStub operationServiceClient =
            newBlockingClient(LongRunningServiceGrpc.newBlockingStub(operationServiceChannel), APP,
                () -> internalUserCredentials.get().token());

        storageServiceChannel = newGrpcChannel(config.getStorage().getAddress(), LzyStorageServiceGrpc.SERVICE_NAME);
        LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient =
            newBlockingClient(LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel), APP,
                () -> internalUserCredentials.get().token());

        channelManagerChannel = newGrpcChannel(channelManagerAddress, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient =
            newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), APP,
                () -> internalUserCredentials.get().token());

        iamChannel = newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);

        SubjectServiceGrpcClient subjectClient =
            new SubjectServiceGrpcClient(APP, iamChannel, config.getIam()::createCredentials);
        AccessBindingServiceGrpcClient abClient =
            new AccessBindingServiceGrpcClient(APP, iamChannel, config.getIam()::createCredentials);

        graphExecutorChannel = newGrpcChannel(config.getGraphExecutorAddress(), GraphExecutorGrpc.SERVICE_NAME);
        GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient =
            newBlockingClient(GraphExecutorGrpc.newBlockingStub(graphExecutorChannel), APP,
                () -> internalUserCredentials.get().token());

        workflowService = new WorkflowService(config, channelManagerClient, allocatorClient,
            operationServiceClient, subjectClient, abClient, storageServiceClient, storage, workflowDao);
        graphExecutionService = new GraphExecutionService(internalUserCredentials, workflowDao, executionDao,
            vmPoolClient, graphExecutorClient, channelManagerClient);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        GrpcChannels.awaitTermination(storageServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(allocatorServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(operationServiceChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(channelManagerChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(graphExecutorChannel, Duration.ofSeconds(10), getClass());
        GrpcChannels.awaitTermination(iamChannel, Duration.ofSeconds(10), getClass());
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
}
