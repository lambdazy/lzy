package ai.lzy.graph;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.graph.model.debug.InjectedFailures;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("GraphExecutorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("GraphExecutorIamToken")
    public RenewableJwt renewableIamToken(ServiceConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Singleton
    @Requires(beans = GraphExecutorDataSource.class)
    @Named("GraphExecutorOperationDao")
    public OperationDao operationDao(GraphExecutorDataSource storage) {
        return new OperationDaoImpl(storage);
    }

    @Singleton
    @Bean(preDestroy = "shutdown")
    @Named("GraphExecutorOperationsExecutor")
    public OperationsExecutor operationsExecutor() {
        final Counter errors = Counter
            .build("executor_errors", "Executor unexpected errors")
            .subsystem("graphexecutor")
            .register();

        return new OperationsExecutor(5, 20, errors::inc, e -> e instanceof InjectedFailures.TerminateException);
    }

    @Singleton
    @Named("GraphExecutorOperationsService")
    public OperationsService operationsService(@Named("GraphExecutorOperationDao") OperationDao operationDao) {
        return new OperationsService(operationDao);
    }

    @Singleton
    @Named("GraphExecutorIdGenerator")
    public IdGenerator idGenerator() {
        return new RandomIdGenerator();
    }
}
