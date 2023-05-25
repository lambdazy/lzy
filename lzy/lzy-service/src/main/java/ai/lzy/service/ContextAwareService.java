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

public interface ContextAwareService {
    LzyServiceContext lzyServiceCtx();

    default LzyServiceValidator validator() {
        return lzyServiceCtx().validator();
    }

    default LzyServiceConfig serviceCfg() {
        return lzyServiceCtx().serviceConfig();
    }

    default PortalServiceSpec portalVmSpec() {
        return lzyServiceCtx().portalServiceSpec();
    }

    default Storage storage() {
        return lzyServiceCtx().storage();
    }

    default WorkflowDao wfDao() {
        return lzyServiceCtx().wfDao();
    }

    default ExecutionDao execDao() {
        return lzyServiceCtx().execDao();
    }

    default GraphDao graphDao() {
        return lzyServiceCtx().graphDao();
    }

    default OperationDao opsDao() {
        return lzyServiceCtx().opsDao();
    }

    default ExecutionOperationsDao execOpsDao() {
        return lzyServiceCtx().execOpsDao();
    }

    default OperationsExecutor opsExecutor() {
        return lzyServiceCtx().opsExecutor();
    }

    default OperationRunnersFactory opRunnersFactory() {
        return lzyServiceCtx().opRunnersFactory();
    }

    default RenewableJwt internalUserCredentials() {
        return lzyServiceCtx().internalUserCredentials();
    }

    default IdGenerator idGenerator() {
        return lzyServiceCtx().idGenerator();
    }

    default GraphExecutorBlockingStub graphsGrpcClient() {
        return lzyServiceCtx().graphsGrpcClient();
    }

    default VmPoolServiceBlockingStub vmPoolGrpcClient() {
        return lzyServiceCtx().vmPoolGrpcClient();
    }

    default KafkaLogsListeners kafkaLogsListeners() {
        return lzyServiceCtx().kafkaLogsListeners();
    }

    default LzyStorageServiceBlockingStub storagesGrpcClient() {
        return lzyServiceCtx().storagesGrpcClient();
    }

    default LongRunningServiceBlockingStub storagesOpsGrpcClient() {
        return lzyServiceCtx().storagesOpsGrpcClient();
    }
}
