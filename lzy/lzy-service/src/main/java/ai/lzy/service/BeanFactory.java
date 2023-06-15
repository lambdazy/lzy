package ai.lzy.service;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoDecorator;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.metrics.DummyMetricReporter;
import ai.lzy.metrics.LogMetricReporter;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.PrometheusMetricReporter;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.config.PortalServiceSpec;
import ai.lzy.service.dao.impl.LzyServiceStorage;
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.NoopKafkaAdminClient;
import ai.lzy.util.kafka.ScramKafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.kafka.S3SinkServiceGrpc;
import ai.lzy.v1.kafka.S3SinkServiceGrpc.S3SinkServiceBlockingStub;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc.LzyStorageServiceBlockingStub;
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
    public static final String TEST_ENV_NAME = "local-tests";

    @Singleton
    @Requires(notEnv = TEST_ENV_NAME)
    public PortalServiceSpec portalVmSpec(LzyServiceConfig serviceCfg) throws IOException, InterruptedException {
        return new PortalServiceSpec(serviceCfg.getPortal().getPoolZone(), serviceCfg.getPortal().getPoolLabel(),
            serviceCfg.getPortal().getDockerImage(), RsaUtils.generateRsaKeys(),
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

    @Singleton
    @Named("LzyServiceStorageGrpcClient")
    public LzyStorageServiceBlockingStub storagesGrpcClient(
        @Named("StorageServiceChannel") ManagedChannel grpcChannel,
        @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(LzyStorageServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceStorageOpsGrpcClient")
    public LongRunningServiceBlockingStub storageOpsGrpcClient(
        @Named("StorageServiceChannel") ManagedChannel grpcChannel,
        @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
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
    @Named("LzySubjectServiceClient")
    public SubjectServiceGrpcClient iamSubjectsGrpcClient(@Named("IamServiceChannel") ManagedChannel iamChannel,
                                                          @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return new SubjectServiceGrpcClient(APP, iamChannel, internalUserCreds::get);
    }

    @Singleton
    @Named("LzyServiceAccessBindingClient")
    public AccessBindingServiceGrpcClient iamAccessClient(@Named("IamServiceChannel") ManagedChannel iamChannel,
                                                          @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return new AccessBindingServiceGrpcClient(APP, iamChannel, internalUserCreds::get);
    }

    @Singleton
    @Named("LzyServiceAllocatorGrpcClient")
    public AllocatorBlockingStub allocatorGrpcClient(@Named("AllocatorServiceChannel") ManagedChannel grpcChannel,
                                                     @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(AllocatorGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceAllocOpsGrpcClient")
    public LongRunningServiceBlockingStub allocOpsClient(@Named("AllocatorServiceChannel") ManagedChannel grpcChannel,
                                                         @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceVmPoolGrpcClient")
    public VmPoolServiceBlockingStub vmPoolClient(@Named("AllocatorServiceChannel") ManagedChannel grpcChannel,
                                                  @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(VmPoolServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceGraphExecutorGrpcClient")
    public GraphExecutorBlockingStub graphExecClient(@Named("GraphExecutorServiceChannel") ManagedChannel grpcChannel,
                                                     @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(GraphExecutorGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServicePrivateChannelsGrpcClient")
    public LzyChannelManagerPrivateBlockingStub channelManagerClient(
        @Named("ChannelManagerServiceChannel") ManagedChannel grpcChannel,
        @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceChannelManagerOpsGrpcClient")
    public LongRunningServiceBlockingStub channelManagerOpClient(
        @Named("ChannelManagerServiceChannel") ManagedChannel grpcChannel,
        @Named("LzyServiceIamToken") RenewableJwt internalUserCreds)
    {
        return newBlockingClient(LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCreds.get().token());
    }

    @Singleton
    @Named("LzyServiceIamToken")
    public RenewableJwt renewableIamToken(LzyServiceConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Singleton
    @Named("LzyServiceAuthInterceptor")
    public AuthServerInterceptor authServerInterceptor(@Named("IamServiceChannel") ManagedChannel iamGrpcChannel) {
        return new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamGrpcChannel));
    }

    @Singleton
    @Named("LzyServiceOperationDao")
    @Requires(notEnv = TEST_ENV_NAME)
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
    @Requires(notEnv = TEST_ENV_NAME)
    public StorageClientFactory storageClientFactory() {
        return new StorageClientFactory(10, 10);
    }

    @Singleton
    @Named("LzyServiceKafkaAdminClient")
    @Requires(notEnv = TEST_ENV_NAME)
    public KafkaAdminClient kafkaAdminClient(LzyServiceConfig config) {
        return new ScramKafkaAdminClient(config.getKafka());
    }

    @Singleton
    @Named("LzyServiceKafkaAdminClient")
    @Requires(env = TEST_ENV_NAME)
    public KafkaAdminClient testKafkaAdminClient(LzyServiceConfig config) {
        return new NoopKafkaAdminClient();
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
    @Named("LzyServiceOperationService")
    public OperationsService operationsService(@Named("LzyServiceOperationDao") OperationDao operationDao) {
        return new OperationsService(operationDao);
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
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    @Bean
    @Requires(env = TEST_ENV_NAME)
    public PortalServiceSpec portalVmSpecForTests(LzyServiceConfig serviceCfg)
        throws IOException, InterruptedException
    {
        return new PortalServiceSpec(serviceCfg.getPortal().getPoolZone(), serviceCfg.getPortal().getPoolLabel(),
            serviceCfg.getPortal().getDockerImage(), RsaUtils.generateRsaKeys(),
            FreePortFinder.find(10001, 11000), FreePortFinder.find(11001, 12000),
            serviceCfg.getPortal().getWorkersPoolSize(), serviceCfg.getPortal().getDownloadsPoolSize(),
            serviceCfg.getPortal().getChunksPoolSize(), serviceCfg.getChannelManagerAddress(),
            serviceCfg.getIam().getAddress(), serviceCfg.getWhiteboardAddress());
    }

    @Singleton
    @Named("LzyServiceOperationDao")
    @Requires(env = TEST_ENV_NAME)
    public OperationDao operationDaoDecorator(LzyServiceStorage storage) {
        return new OperationDaoDecorator(storage);
    }

    @Singleton
    @Named("LzyServiceIdGenerator")
    public IdGenerator idGenerator() {
        return new RandomIdGenerator();
    }

    @Bean(preDestroy = "destroy")
    @Singleton
    @Named("LzyServiceStorageClientFactory")
    @Requires(env = TEST_ENV_NAME)
    public StorageClientFactory storageClientFactoryForTests() {
        return new StorageClientFactory(2, 2);
    }
}
