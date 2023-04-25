package ai.lzy.kafka;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.common.UUIDIdGenerator;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Factory
public class BeanFactory {

    @Singleton
    @Named("S3SinkKafkaHelper")
    public KafkaHelper helper(ServiceConfig config) {
        return new KafkaHelper(config.getKafka());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("S3SinkIamChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return newGrpcChannel(config.getIam().getAddress(), LzySubjectServiceGrpc.SERVICE_NAME,
            LzyAccessBindingServiceGrpc.SERVICE_NAME);
    }
}
