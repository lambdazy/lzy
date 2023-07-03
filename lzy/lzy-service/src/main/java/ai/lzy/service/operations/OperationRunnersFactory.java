package ai.lzy.service.operations;

import ai.lzy.common.IdGenerator;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.dao.*;
import ai.lzy.service.dao.impl.LzyServiceStorage;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.operations.graph.ExecuteGraph;
import ai.lzy.service.operations.start.StartExecution;
import ai.lzy.service.operations.stop.AbortExecution;
import ai.lzy.service.operations.stop.FinishExecution;
import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class OperationRunnersFactory {
    private static final Logger LOG = LogManager.getLogger(OperationRunnersFactory.class);

    private final LzyServiceConfig serviceConfig;
    private final Storage storage;
    private final WorkflowDao wfDao;
    private final ExecutionDao execDao;
    private final GraphDao graphDao;
    private final OperationDao opDao;
    private final ExecutionOperationsDao execOpsDao;
    private final OperationsExecutor executor;

    private final KafkaAdminClient kafkaAdminClient;
    private final KafkaLogsListeners kafkaLogsListeners;
    private final AllocatorBlockingStub allocClient;
    private final BeanFactory.S3SinkClient s3SinkClient;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final SubjectServiceGrpcClient subjectClient;
    private final GraphExecutorBlockingStub graphsClient;
    private final LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final RenewableJwt internalUserCredentials;

    private final StorageClientFactory storageClientFactory;
    private final VmPoolServiceBlockingStub vmPoolClient;
    private final KafkaConfig kafkaConfig;
    private final IdGenerator idGenerator;
    private final LzyServiceMetrics metrics;

    public OperationRunnersFactory(LzyServiceStorage storage, WorkflowDao wfDao, ExecutionDao execDao,
                                   @Named("LzyServiceOperationDao") OperationDao opDao, GraphDao graphDao,
                                   @Named("LzyServiceOperationsExecutor") OperationsExecutor executor,
                                   ExecutionOperationsDao execOpsDao, KafkaAdminClient kafkaAdminClient,
                                   KafkaLogsListeners kafkaLogsListeners, BeanFactory.S3SinkClient s3SinkClient,
                                   @Named("LzyServiceAllocatorGrpcClient") AllocatorBlockingStub allocClient,
                                   @Named("LzyServiceAllocOpsGrpcClient") LongRunningServiceBlockingStub allocOpClient,
                                   @Named("LzySubjectServiceClient") SubjectServiceGrpcClient subjectClient,
                                   @Named("LzyServiceGraphExecutorGrpcClient") GraphExecutorBlockingStub graphsClient,
                                   @Named("LzyServicePrivateChannelsGrpcClient")
                                       LzyChannelManagerPrivateBlockingStub channelManagerClient,
                                   @Named("LzyServiceChannelManagerOpsGrpcClient")
                                       LongRunningServiceBlockingStub channelManagerOpClient,
                                   @Named("LzyServiceStorageClientFactory") StorageClientFactory storageClientFactory,
                                   @Named("LzyServiceVmPoolGrpcClient") VmPoolServiceBlockingStub vmPoolClient,
                                   @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
                                   LzyServiceMetrics metrics, LzyServiceConfig config,
                                   @Named("LzyServiceIdGenerator") IdGenerator idGenerator)
    {
        this.serviceConfig = config;
        this.storage = storage;
        this.wfDao = wfDao;
        this.execDao = execDao;
        this.opDao = opDao;
        this.graphDao = graphDao;
        this.execOpsDao = execOpsDao;
        this.executor = executor;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaLogsListeners = kafkaLogsListeners;
        this.allocClient = allocClient;
        this.s3SinkClient = s3SinkClient;
        this.allocOpClient = allocOpClient;
        this.subjectClient = subjectClient;
        this.graphsClient = graphsClient;
        this.channelManagerClient = channelManagerClient;
        this.internalUserCredentials = internalUserCredentials;
        this.storageClientFactory = storageClientFactory;
        this.metrics = metrics;
        this.vmPoolClient = vmPoolClient;
        this.kafkaConfig = config.getKafka();
        this.idGenerator = idGenerator;
    }

    public StartExecution createStartExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                  String userId, String wfName, String execId)
        throws Exception
    {
        LMST.StorageConfig storageConfig = withRetries(LOG, () -> execDao.getStorageConfig(execId, null));

        return StartExecution.builder()
            .setServiceConfig(serviceConfig)
            .setId(opId)
            .setDescription(opDesc)
            .setUserId(userId)
            .setWfName(wfName)
            .setExecId(execId)
            .setStorage(storage)
            .setWfDao(wfDao)
            .setExecDao(execDao)
            .setGraphDao(graphDao)
            .setOperationsDao(opDao)
            .setExecOpsDao(execOpsDao)
            .setExecutor(executor)
            .setState(StartExecutionState.initial())
            .setStorageCfg(storageConfig)
            .setIdempotencyKey(idempotencyKey)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setKafkaClient(kafkaAdminClient)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setInternalUserCredentials(internalUserCredentials)
            .setOpRunnersFactory(this)
            .build();
    }

    public FinishExecution createFinishExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                    String userId, String wfName, String execId)
        throws Exception
    {
        StopExecutionState state = withRetries(LOG, () -> execDao.loadStopExecState(execId, null));

        return FinishExecution.builder()
            .setServiceConfig(serviceConfig)
            .setId(opId)
            .setDescription(opDesc)
            .setUserId(userId)
            .setWfName(wfName)
            .setExecId(execId)
            .setStorage(storage)
            .setWfDao(wfDao)
            .setExecDao(execDao)
            .setGraphDao(graphDao)
            .setOperationsDao(opDao)
            .setExecOpsDao(execOpsDao)
            .setExecutor(executor)
            .setState(state)
            .setIdempotencyKey(idempotencyKey)
            .setChannelsClient(channelManagerClient)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setAllocOpClient(allocOpClient)
            .setKafkaClient(kafkaAdminClient)
            .setKafkaLogsListeners(kafkaLogsListeners)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setInternalUserCredentials(internalUserCredentials)
            .setOpRunnersFactory(this)
            .build();
    }

    public AbortExecution createAbortExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                  @Nullable String userId, String wfName, String execId)
        throws Exception
    {
        StopExecutionState state = withRetries(LOG, () -> execDao.loadStopExecState(execId, null));

        return AbortExecution.builder()
            .setServiceConfig(serviceConfig)
            .setId(opId)
            .setDescription(opDesc)
            .setUserId(userId)
            .setWfName(wfName)
            .setExecId(execId)
            .setStorage(storage)
            .setWfDao(wfDao)
            .setExecDao(execDao)
            .setGraphDao(graphDao)
            .setOperationsDao(opDao)
            .setExecOpsDao(execOpsDao)
            .setExecutor(executor)
            .setState(state)
            .setIdempotencyKey(idempotencyKey)
            .setChannelsClient(channelManagerClient)
            .setGraphClient(graphsClient)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setAllocOpClient(allocOpClient)
            .setKafkaClient(kafkaAdminClient)
            .setKafkaLogsListeners(kafkaLogsListeners)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setInternalUserCredentials(internalUserCredentials)
            .setOpRunnersFactory(this)
            .build();
    }

    public ExecuteGraph createExecuteGraphOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                   String userId, String wfName, String execId) throws Exception
    {
        ExecutionDao.ExecuteGraphData execGraphData = withRetries(LOG, () -> execDao.loadExecGraphData(execId, null));

        StorageClient storageClient = storageClientFactory.provider(execGraphData.storageConfig()).get();

        ExecuteGraphState state = withRetries(LOG, () -> execOpsDao.getState(opId, null));

        return ExecuteGraph.builder()
            .setServiceConfig(serviceConfig)
            .setId(opId)
            .setDescription(opDesc)
            .setUserId(userId)
            .setWfName(wfName)
            .setExecId(execId)
            .setStorage(storage)
            .setWfDao(wfDao)
            .setExecDao(execDao)
            .setGraphDao(graphDao)
            .setOperationsDao(opDao)
            .setExecOpsDao(execOpsDao)
            .setExecutor(executor)
            .setState(state)
            .setKafkaTopicDesc(execGraphData.kafkaTopicDesc())
            .setKafkaConfig(kafkaConfig)
            .setIdempotencyKey(idempotencyKey)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setGraphsClient(graphsClient)
            .setChannelsClient(channelManagerClient)
            .setStorageClient(storageClient)
            .setVmPoolClient(vmPoolClient)
            .setKafkaClient(kafkaAdminClient)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setInternalUserCredentials(internalUserCredentials)
            .setOpRunnersFactory(this)
            .build();
    }
}
