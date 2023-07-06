package ai.lzy.service;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.service.dao.impl.LzyServiceStorage;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.operations.OperationRunnersFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public record LzyServiceContext(
    ApplicationContext micronautCtx,
    LzyServiceConfig serviceConfig,
    LzyServiceStorage storage,
    WorkflowDao wfDao,
    ExecutionDao execDao,
    GraphDao graphDao,
    @Named("LzyServiceOperationDao") OperationDao opsDao,
    ExecutionOperationsDao execOpsDao,
    @Named("LzyServiceOperationsExecutor") OperationsExecutor opsExecutor,
    OperationRunnersFactory opRunnersFactory,
    LzyServiceValidator validator,
    @Named("LzyServiceIdGenerator") IdGenerator idGenerator,
    @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials,
    @Named("LzyServiceGraphExecutorGrpcClient") GraphExecutorBlockingStub graphsGrpcClient,
    KafkaLogsListeners kafkaLogsListeners,
    @Named("LzyServiceVmPoolGrpcClient") VmPoolServiceBlockingStub vmPoolGrpcClient,
    @Named("LzyServiceStorageGrpcClient") LzyStorageServiceBlockingStub storagesGrpcClient,
    @Named("LzyServiceStorageOpsGrpcClient") LongRunningServiceBlockingStub storagesOpsGrpcClient
)
{
}
