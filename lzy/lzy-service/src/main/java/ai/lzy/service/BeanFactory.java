package ai.lzy.service;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.metrics.DummyMetricReporter;
import ai.lzy.metrics.LogMetricReporter;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.PrometheusMetricReporter;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.config.PortalVmSpec;
import ai.lzy.service.dao.impl.LzyServiceStorage;
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.NoopKafkaAdminClient;
import ai.lzy.util.kafka.ScramKafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import ai.lzy.v1.kafka.S3SinkServiceGrpc.S3SinkServiceBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Level;

import java.io.IOException;

import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Factory
public class BeanFactory {
    @Singleton
    public PortalVmSpec portalVmSpec(LzyServiceConfig serviceCfg) throws IOException, InterruptedException {
        // var portalPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : serviceCfg.getPortal().getPortalApiPort();
        // var slotsApiPort = PEEK_RANDOM_PORTAL_PORTS ? -1 : serviceCfg.getPortal().getSlotsApiPort();
        return new PortalVmSpec(serviceCfg.getPortal().getPoolZone(), serviceCfg.getPortal().getPoolLabel(),
            serviceCfg.getPortal().getDockerImage(), RsaUtils.generateRsaKeys().privateKey(),
            serviceCfg.getPortal().getPortalApiPort(), serviceCfg.getPortal().getSlotsApiPort(),
            serviceCfg.getPortal().getWorkersPoolSize(), serviceCfg.getPortal().getDownloadsPoolSize(),
            serviceCfg.getPortal().getChunksPoolSize(), serviceCfg.getChannelManagerAddress(),
            serviceCfg.getIam().getAddress(), serviceCfg.getWhiteboardAddress());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorServiceChannel")
    public ManagedChannel allocatorChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME, VmPoolServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("StorageServiceChannel")
    public ManagedChannel storageChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getStorage().getAddress(), LzyStorageServiceGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("ChannelManagerServiceChannel")
    public ManagedChannel channelManagerChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("IamServiceChannel")
    public ManagedChannel iamChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getIam().getAddress(), LzySubjectServiceGrpc.SERVICE_NAME,
            LzyAccessBindingServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("GraphExecutorServiceChannel")
    public ManagedChannel graphExecutorChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getGraphExecutorAddress(), GraphExecutorGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("LzyServiceIamToken")
    public RenewableJwt renewableIamToken(LzyServiceConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Singleton
    @Named("LzyServiceOperationDao")
    @Requires(notEnv = "test-mock")
    public OperationDao operationDao(LzyServiceStorage storage) {
        return new OperationDaoImpl(storage);
    }

    @Singleton
    @Named("LzyServiceObjectMapper")
    public ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new ProtobufModule());
    }

    @Singleton
    @Bean(preDestroy = "stop")
    @Named("LzyServiceMetricReporter")
    public MetricReporter metricReporter(LzyServiceConfig.MetricsConfig config) {
        CollectorRegistry.defaultRegistry.clear();

        return switch (config.getKind()) {
            case Disabled -> new DummyMetricReporter();
            case Logger -> new LogMetricReporter(config.getLoggerName(),
                Level.valueOf(config.getLoggerLevel().toUpperCase()));
            case Prometheus -> new PrometheusMetricReporter(config.getPort());
        };
    }

    @Bean(preDestroy = "destroy")
    @Singleton
    @Named("LzyServiceStorageClientFactory")
    public StorageClientFactory storageClientFactory() {
        return new StorageClientFactory(10, 10);
    }

    @Singleton
    @Named("LzyServiceKafkaAdminClient")
    @Requires(notEnv = "test")
    public KafkaAdminClient kafkaAdminClient(LzyServiceConfig config) {
        return new ScramKafkaAdminClient(config.getKafka());
    }

    @Singleton
    @Named("LzyServiceKafkaAdminClient")
    @Requires(env = "test")
    public KafkaAdminClient testKafkaAdminClient(LzyServiceConfig config) {
        return new NoopKafkaAdminClient();
    }

    @Singleton
    @Named("LzySubjectServiceClient")
    public SubjectServiceGrpcClient subjectServiceGrpcClient(@Named("IamServiceChannel") ManagedChannel iamChannel,
                                                             @Named("LzyServiceIamToken") RenewableJwt userCreds)
    {
        return new SubjectServiceGrpcClient(APP, iamChannel, userCreds::get);
    }

    @Singleton
    @Bean(preDestroy = "shutdown")
    @Named("LzyServiceOperationsExecutor")
    public OperationsExecutor operationsExecutor(@Named("LzyServiceMetricReporter") MetricReporter mr) {
        final Counter errors = Counter
            .build("executor_errors", "Executor unexpected errors")
            .subsystem("allocator")
            .register();

        return new OperationsExecutor(5, 20, errors::inc, e -> e instanceof InjectedFailures.TerminateException);
    }

    @Singleton
    public static class S3SinkClient {
        private final boolean enabled;
        private final S3SinkServiceBlockingStub stub;
        private final ManagedChannel channel;

        public S3SinkClient(@Named("LzyServiceIamToken") RenewableJwt userCreds, LzyServiceConfig config) {
            enabled = config.getS3SinkAddress() != null;

            if (!enabled) {
                stub = null;
                channel = null;
                return;
            }

            channel = newGrpcChannel(config.getS3SinkAddress(), S3SinkServiceGrpc.SERVICE_NAME);

            stub = newBlockingClient(S3SinkServiceGrpc.newBlockingStub(channel),
                S3SinkServiceGrpc.SERVICE_NAME,
                () -> userCreds.get().token());

        }

        public boolean enabled() {
            return enabled;
        }

        public S3SinkServiceBlockingStub stub() {
            if (!enabled) {
                throw Status.INTERNAL
                    .withDescription("Trying to get stub of S3Sink, but it is not enabled")
                    .asRuntimeException();
            }
            return stub;
        }

        @PreDestroy
        public void close() {
            channel.shutdownNow();
        }
    }
}
