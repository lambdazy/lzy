package ai.lzy.service;

import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.service.data.dao.GraphDao;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.graph.execute.ExecuteGraphAction;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.workflow.finish.AbortExecutionAction;
import ai.lzy.service.workflow.finish.FinishExecutionAction;
import ai.lzy.service.workflow.start.StartExecutionAction;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Singleton
public final class ActionsManager {
    private final Storage storage;
    private final WorkflowDao wfDao;
    private final ExecutionDao execDao;
    private final OperationDao opDao;
    private final GraphDao graphDao;
    private final OperationsExecutor executor;

    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaLogsListeners kafkaLogsListeners;
    private final AllocatorBlockingStub allocClient;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final GraphExecutorBlockingStub graphsClient;
    private final LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final LongRunningServiceBlockingStub channelManagerOpClient;
    private final RenewableJwt internalUserCredentials;
    private final String portalStdoutChannelName;
    private final String portalStderrChannelName;
    private final Duration allocateVmCacheTimeout;
    private final boolean isKafkaEnabled;
    private final StorageClientFactory storageClientFactory;
    private final LzyServiceMetrics metrics;
    private final VmPoolServiceBlockingStub vmPoolClient;
    private final KafkaConfig kafkaConfig;

    public ActionsManager(Storage storage, WorkflowDao wfDao, ExecutionDao execDao, OperationDao opDao,
                          GraphDao graphDao, OperationsExecutor executor, KafkaAdminClient kafkaAdminClient,
                          KafkaLogsListeners kafkaLogsListeners, AllocatorBlockingStub allocClient,
                          LongRunningServiceBlockingStub allocOpClient, SubjectServiceGrpcClient subjectClient,
                          AccessBindingServiceGrpcClient abClient, GraphExecutorBlockingStub graphsClient,
                          LzyChannelManagerPrivateBlockingStub channelManagerClient,
                          LongRunningServiceBlockingStub channelManagerOpClient,
                          RenewableJwt internalUserCredentials,
                          String portalStdoutChannelName, String portalStderrChannelName,
                          Duration allocateVmCacheTimeout, boolean isKafkaEnabled,
                          StorageClientFactory storageClientFactory, LzyServiceMetrics metrics,
                          VmPoolServiceBlockingStub vmPoolClient, KafkaConfig kafkaConfig)
    {
        this.storage = storage;
        this.wfDao = wfDao;
        this.execDao = execDao;
        this.opDao = opDao;
        this.graphDao = graphDao;
        this.executor = executor;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.allocClient = allocClient;
        this.allocOpClient = allocOpClient;
        this.subjectClient = subjectClient;
        this.abClient = abClient;
        this.graphsClient = graphsClient;
        this.channelManagerClient = channelManagerClient;
        this.channelManagerOpClient = channelManagerOpClient;
        this.internalUserCredentials = internalUserCredentials;
        this.portalStdoutChannelName = portalStdoutChannelName;
        this.portalStderrChannelName = portalStderrChannelName;
        this.allocateVmCacheTimeout = allocateVmCacheTimeout;
        this.isKafkaEnabled = isKafkaEnabled;
        this.storageClientFactory = storageClientFactory;
        this.metrics = metrics;
        this.vmPoolClient = vmPoolClient;
        this.kafkaConfig = kafkaConfig;
    }

    public void startExecutionAction(String opId, String opDesc, @Nullable String idempotencyKey,
                                     String userId, String wfName, String execId)
    {
        executor.startNew(new StartExecutionAction(opId, opDesc, userId, wfName, execId, storage, opDao, wfDao,
            execDao, executor, isKafkaEnabled, portalStdoutChannelName, portalStderrChannelName, allocateVmCacheTimeout,
            idempotencyKey, subjectClient, abClient, kafkaAdminClient, allocClient, allocOpClient, channelManagerClient,
            this, metrics));
    }

    public void finishExecutionAction(String opId, String opDesc, @Nullable String idempotencyKey,
                                      String execId, Status finishStatus)
    {
        executor.startNew(new FinishExecutionAction(opId, opDesc, execId, executor, storage, opDao, execDao,
            finishStatus, internalUserCredentials, idempotencyKey, channelManagerClient, channelManagerOpClient,
            allocClient, allocOpClient, subjectClient, kafkaAdminClient, kafkaLogsListeners));
    }

    public void abortExecutionAction(String opId, String opDesc, @Nullable String idempotencyKey,
                                     String execId, Status finishStatus)
    {
        executor.startNew(new AbortExecutionAction(opId, opDesc, execId, executor, storage, opDao, execDao, graphDao,
            finishStatus, internalUserCredentials, idempotencyKey, graphsClient, channelManagerClient, allocClient,
            subjectClient, kafkaAdminClient, kafkaLogsListeners));
    }

    public void executeGraphAction(String opId, String opDesc, @Nullable String idempotencyKey,
                                   String userId, String wfName, String execId, @Nullable String poolsZone,
                                   List<String> opsPoolSpec, Map<String, LWF.DataDescription> slot2description,
                                   List<LWF.Operation> operations)
    {
        executor.startNew(new ExecuteGraphAction(opId, opDesc, userId, wfName, execId, storage, opDao, wfDao, execDao,
            graphDao, executor, poolsZone, opsPoolSpec, slot2description, operations, kafkaConfig,
            internalUserCredentials, idempotencyKey, storageClientFactory, vmPoolClient, channelManagerClient,
            graphsClient, this));
    }
}
