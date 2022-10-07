package ai.lzy.graph;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("GraphExecutorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(config.getAuth().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }

}
