package ai.lzy.whiteboard;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Named("WhiteboardIamGrpcChannel")
    public ManagedChannel iamChannel(AppConfig config) {
        return ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }
}
