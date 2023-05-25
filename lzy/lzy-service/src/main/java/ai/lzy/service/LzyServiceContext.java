package ai.lzy.service;

import ai.lzy.common.IdGenerator;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.config.PortalServiceSpec;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.service.kafka.KafkaLogsListeners;
import ai.lzy.service.operations.OperationRunnersFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;

public record LzyServiceContext(LzyServiceConfig serviceConfig, PortalServiceSpec portalServiceSpec, Storage storage,
                                WorkflowDao wfDao, ExecutionDao execDao, GraphDao graphDao, OperationDao opsDao,
                                ExecutionOperationsDao execOpsDao, OperationsExecutor opsExecutor,
                                OperationRunnersFactory opRunnersFactory, LzyServiceValidator validator,
                                IdGenerator idGenerator, RenewableJwt internalUserCredentials,
                                GraphExecutorBlockingStub graphsGrpcClient, KafkaLogsListeners kafkaLogsListeners,
                                VmPoolServiceBlockingStub vmPoolGrpcClient,
                                LzyStorageServiceBlockingStub storagesGrpcClient,
                                LongRunningServiceBlockingStub storagesOpsGrpcClient) {}
