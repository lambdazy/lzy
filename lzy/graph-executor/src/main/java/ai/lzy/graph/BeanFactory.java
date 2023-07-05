package ai.lzy.graph;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.impl.GraphExecutorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {
    public static final String TEST_ENV_NAME = "local-test";
    public static final String GRAPH_EXEC_DECORATOR_ENV_NAME = "ge-decorator";

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
}
