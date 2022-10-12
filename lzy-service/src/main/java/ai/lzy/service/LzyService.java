package ai.lzy.service;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.service.graph.GraphExecutionService;
import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.v1.workflow.LWFS.*;

@Singleton
public class LzyService extends LzyWorkflowServiceGrpc.LzyWorkflowServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LzyService.class);

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
        JwtCredentials internalUserCredentials = config.getIam().createCredentials();

        LOG.info("Init Internal User '{}' credentials", config.getIam().getInternalUserName());

        var allocatorAddress = HostAndPort.fromString(config.getAllocatorAddress());

        allocatorServiceChannel = newGrpcChannel(allocatorAddress, AllocatorGrpc.SERVICE_NAME);
        AllocatorGrpc.AllocatorBlockingStub allocatorClient =
            newBlockingClient(AllocatorGrpc.newBlockingStub(allocatorServiceChannel),
                internalUserCredentials::token);
        VmPoolServiceGrpc.VmPoolServiceBlockingStub vmPoolClient =
            newBlockingClient(VmPoolServiceGrpc.newBlockingStub(allocatorServiceChannel),
                internalUserCredentials::token);

        operationServiceChannel = newGrpcChannel(allocatorAddress, OperationServiceApiGrpc.SERVICE_NAME);
        OperationServiceApiGrpc.OperationServiceApiBlockingStub operationServiceClient =
            newBlockingClient(OperationServiceApiGrpc.newBlockingStub(operationServiceChannel),
                internalUserCredentials::token);

        storageServiceChannel = newGrpcChannel(config.getStorage().getAddress(), LzyStorageServiceGrpc.SERVICE_NAME);
        LzyStorageServiceGrpc.LzyStorageServiceBlockingStub storageServiceClient =
            newBlockingClient(LzyStorageServiceGrpc.newBlockingStub(storageServiceChannel),
                internalUserCredentials::token);

        channelManagerChannel = newGrpcChannel(channelManagerAddress, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManagerClient =
            newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel),
                internalUserCredentials::token);

        iamChannel = newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);

        SubjectServiceGrpcClient subjectClient =
            new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
        AccessBindingServiceGrpcClient abClient =
            new AccessBindingServiceGrpcClient(iamChannel, config.getIam()::createCredentials);

        graphExecutorChannel = newGrpcChannel(config.getGraphExecutorAddress(), GraphExecutorGrpc.SERVICE_NAME);
        GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient =
            newBlockingClient(GraphExecutorGrpc.newBlockingStub(graphExecutorChannel),
                internalUserCredentials::token);

        workflowService = new WorkflowService(config, channelManagerClient, allocatorClient,
            operationServiceClient, subjectClient, abClient, storageServiceClient, storage, workflowDao);
        graphExecutionService = new GraphExecutionService(internalUserCredentials, workflowDao, executionDao,
            vmPoolClient, graphExecutorClient, channelManagerClient);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown WorkflowService.");
        storageServiceChannel.shutdown();
        allocatorServiceChannel.shutdown();
        operationServiceChannel.shutdown();
        channelManagerChannel.shutdown();
        graphExecutorChannel.shutdown();
        iamChannel.shutdown();
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
