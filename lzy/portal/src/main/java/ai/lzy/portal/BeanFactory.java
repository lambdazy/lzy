package ai.lzy.portal;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.portal.config.PortalConfig;
import ai.lzy.portal.services.PortalService;
import ai.lzy.portal.services.PortalSlotsService;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.annotation.Nonnull;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static ai.lzy.portal.services.PortalService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalChannelManagerChannel")
    public ManagedChannel channelManager(PortalConfig config) {
        return newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalIamChannel")
    public ManagedChannel iamChannel(PortalConfig config) {
        return newGrpcChannel(config.getIamAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("PortalTokenSupplier")
    public Supplier<String> tokenFactory(PortalConfig config)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        var privateKey = CredentialsUtils.readPrivateKey(config.getIamPrivateKey());
        var slotsJwt = new RenewableJwt(config.getPortalId(), "INTERNAL", Duration.ofHours(1), privateKey);
        return () -> slotsJwt.get().token();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalGrpcServer")
    public Server portalServer(PortalConfig config, PortalService portalService,
                               @Named("PortalOperationsService") LocalOperationService operationService,
                               @Named("PortalIamChannel") ManagedChannel iamChannel)
    {
        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);
        return newGrpcServer(config.getHost(), config.getPortalApiPort(),
            new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .addService(ServerInterceptors.intercept(portalService, internalOnly))
            .addService(operationService)
            .build();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalSlotsGrpcServer")
    public Server portalSlotsServer(PortalConfig config, PortalSlotsService slotsApi,
                                    @Named("PortalOperationsService") LocalOperationService operationService,
                                    @Named("PortalIamChannel") ManagedChannel iamChannel)
    {
        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        return newGrpcServer(config.getHost(), config.getSlotsApiPort(), auth)
            .addService(slotsApi)
            .addService(operationService)
            .build();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalAllocatorAgent")
    public AllocatorAgent allocatorAgent(PortalConfig config) {
        return new AllocatorAgent(config.getAllocatorToken(), config.getVmId(), config.getAllocatorAddress(),
            config.getAllocatorHeartbeatPeriod());
    }

    @Singleton
    @Named("PortalOperationsService")
    public LocalOperationService portalOperations(PortalConfig config) {
        return new LocalOperationService("Portal-" + config.getPortalId());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalServiceExecutor")
    public ExecutorService workersPool(PortalConfig config) {
        var poolSize = config.getConcurrency().getWorkersPoolSize();
        return new ThreadPoolExecutor(poolSize / 2, poolSize, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private static final Logger LOG = LogManager.getLogger(PortalService.class);

                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "lr-slots-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
    }

    @Bean(preDestroy = "destroy")
    @Singleton
    @Named("PortalStorageClientFactory")
    public StorageClientFactory storageClientFactory(PortalConfig config) {
        return new StorageClientFactory(config.getConcurrency().getDownloadsPoolSize(),
            config.getConcurrency().getChunksPoolSize());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("PortalS3UploadPool")
    public ExecutorService s3UploadPool() {
        var counter = new AtomicInteger(1);
        return new ThreadPoolExecutor(1, 10, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            r -> new Thread(r, "s3-uploader-" + counter.getAndIncrement()));
    }
}
