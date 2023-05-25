package ai.lzy.service.operations;

import ai.lzy.common.IdGenerator;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.config.AllocatorSessionSpec;
import ai.lzy.service.config.PortalServiceSpec;
import ai.lzy.service.dao.*;
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
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.portal.LzyPortalGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class OperationRunnersFactory {
    private static final Logger LOG = LogManager.getLogger(OperationRunnersFactory.class);

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
    private final AccessBindingServiceGrpcClient abClient;
    private final GraphExecutorBlockingStub graphsClient;
    private final LzyChannelManagerPrivateBlockingStub channelManagerClient;
    private final LongRunningServiceBlockingStub channelManagerOpClient;
    private final RenewableJwt internalUserCredentials;

    private final StorageClientFactory storageClientFactory;
    private final VmPoolServiceBlockingStub vmPoolClient;
    private final KafkaConfig kafkaConfig;
    private final IdGenerator idGenerator;
    private final LzyServiceMetrics metrics;

    public OperationRunnersFactory(Storage storage, WorkflowDao wfDao, ExecutionDao execDao, OperationDao opDao,
                                   GraphDao graphDao, ExecutionOperationsDao execOpsDao,
                                   OperationsExecutor executor, KafkaAdminClient kafkaAdminClient,
                                   KafkaLogsListeners kafkaLogsListeners, AllocatorBlockingStub allocClient,
                                   BeanFactory.S3SinkClient s3SinkClient,
                                   LongRunningServiceBlockingStub allocOpClient, SubjectServiceGrpcClient subjectClient,
                                   AccessBindingServiceGrpcClient abClient, GraphExecutorBlockingStub graphsClient,
                                   LzyChannelManagerPrivateBlockingStub channelManagerClient,
                                   LongRunningServiceBlockingStub channelManagerOpClient,
                                   RenewableJwt internalUserCredentials,
                                   StorageClientFactory storageClientFactory, LzyServiceMetrics metrics,
                                   VmPoolServiceBlockingStub vmPoolClient, KafkaConfig kafkaConfig,
                                   IdGenerator idGenerator)
    {
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
        this.abClient = abClient;
        this.graphsClient = graphsClient;
        this.channelManagerClient = channelManagerClient;
        this.channelManagerOpClient = channelManagerOpClient;
        this.internalUserCredentials = internalUserCredentials;
        this.storageClientFactory = storageClientFactory;
        this.metrics = metrics;
        this.vmPoolClient = vmPoolClient;
        this.kafkaConfig = kafkaConfig;
        this.idGenerator = idGenerator;
    }

    public StartExecution createStartExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                  String userId, String wfName, String execId,
                                                  Duration allocateVmCacheTimeout, PortalServiceSpec portalServiceSpec)
        throws Exception
    {
        LMST.StorageConfig storageConfig = withRetries(LOG, () -> execDao.getStorageConfig(execId, null));

        var allocatorSessionSpec = new AllocatorSessionSpec(userId, "session for exec with id=" + execId,
            VmAllocatorApi.CachePolicy.newBuilder()
                .setIdleTimeout(Durations.fromSeconds(allocateVmCacheTimeout.getSeconds()))
                .build());

        return StartExecution.builder()
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
            .setAllocatorSessionSpec(allocatorSessionSpec)
            .setPortalVmSpec(portalServiceSpec)
            .setStorageCfg(storageConfig)
            .setIdempotencyKey(idempotencyKey)
            .setAbClient(abClient)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setAllocOpClient(allocOpClient)
            .setKafkaClient(kafkaAdminClient)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setOpRunnersFactory(this)
            .build();
    }

    public FinishExecution createFinishExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                    String userId, String wfName, String execId, Status finishStatus)
        throws Exception
    {
        final StopExecutionState[] state = {null};
        final String[] portalVmAddress = {null};

        withRetries(LOG, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                state[0] = execDao.loadStopExecState(execId, tx);
                portalVmAddress[0] = execDao.getPortalVmAddress(execId, tx);
            }
        });

        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(newGrpcChannel(portalVmAddress[0],
            LzyPortalGrpc.SERVICE_NAME)), APP, () -> internalUserCredentials.get().token());
        var portalOpClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(newGrpcChannel(portalVmAddress[0],
            LongRunningServiceGrpc.SERVICE_NAME)), APP, () -> internalUserCredentials.get().token());

        return FinishExecution.builder()
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
            .setState(state[0])
            .setFinishStatus(finishStatus)
            .setIdempotencyKey(idempotencyKey)
            .setPortalClient(portalClient)
            .setPortalOpClient(portalOpClient)
            .setChannelsClient(channelManagerClient)
            .setChannelsOpClient(channelManagerOpClient)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setAllocOpClient(allocOpClient)
            .setKafkaClient(kafkaAdminClient)
            .setKafkaLogsListeners(kafkaLogsListeners)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setOpRunnersFactory(this)
            .build();
    }

    public AbortExecution createAbortExecOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                  @Nullable String userId, @Nullable String wfName,
                                                  String execId, Status finishStatus)
        throws Exception
    {
        final StopExecutionState[] state = {null};
        final String[] portalVmAddress = {null};

        withRetries(LOG, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                state[0] = execDao.loadStopExecState(execId, tx);
                portalVmAddress[0] = execDao.getPortalVmAddress(execId, tx);
            }
        });

        var portalClient = newBlockingClient(LzyPortalGrpc.newBlockingStub(newGrpcChannel(portalVmAddress[0],
            LzyPortalGrpc.SERVICE_NAME)), APP, () -> internalUserCredentials.get().token());
        var portalOpClient = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(newGrpcChannel(portalVmAddress[0],
            LongRunningServiceGrpc.SERVICE_NAME)), APP, () -> internalUserCredentials.get().token());

        return AbortExecution.builder()
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
            .setState(state[0])
            .setFinishStatus(finishStatus)
            .setIdempotencyKey(idempotencyKey)
            .setPortalClient(portalClient)
            .setPortalOpClient(portalOpClient)
            .setChannelsClient(channelManagerClient)
            .setGraphClient(graphsClient)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setKafkaClient(kafkaAdminClient)
            .setKafkaLogsListeners(kafkaLogsListeners)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setOpRunnersFactory(this)
            .build();
    }

    public ExecuteGraph createExecuteGraphOpRunner(String opId, String opDesc, @Nullable String idempotencyKey,
                                                   String userId, String wfName, String execId,
                                                   LWFS.ExecuteGraphRequest request) throws Exception
    {
        final LMST.StorageConfig[] storageConfig = {null};
        final ExecutionDao.KafkaTopicDesc[] kafkaTopicDesc = {null};

        withRetries(LOG, () -> {
            try (var tx = TransactionHandle.create(storage)) {
                storageConfig[0] = execDao.getStorageConfig(execId, tx);
                kafkaTopicDesc[0] = execDao.getKafkaTopicDesc(execId, tx);
            }
        });

        ExecuteGraphState state = new ExecuteGraphState(request.getGraph());
        StorageClient storageClient = storageClientFactory.provider(storageConfig[0]).get();

        return ExecuteGraph.builder()
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
            .setKafkaTopicDesc(kafkaTopicDesc[0])
            .setKafkaConfig(kafkaConfig)
            .setIdempotencyKey(idempotencyKey)
            .setSubjClient(subjectClient)
            .setAllocClient(allocClient)
            .setGraphsClient(graphsClient)
            .setStorageClient(storageClient)
            .setVmPoolClient(vmPoolClient)
            .setKafkaClient(kafkaAdminClient)
            .setS3SinkClient(s3SinkClient)
            .setIdGenerator(idGenerator)
            .setMetrics(metrics)
            .setOpRunnersFactory(this)
            .build();
    }
}
