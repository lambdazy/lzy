package ai.lzy.service;

import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.portal.LzyPortalGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class PortalClientProvider {
    private static final Logger LOG = LogManager.getLogger();

    private final ExecutionDao executionDao;
    private final RenewableJwt internalUserCredentials;
    private final Map<String, ManagedChannel> portalChannels = new ConcurrentHashMap<>();

    public PortalClientProvider(ExecutionDao executionDao,
                                @Named("LzyServiceIamToken") RenewableJwt internalUserCredentials)
    {
        this.executionDao = executionDao;
        this.internalUserCredentials = internalUserCredentials;
    }

    @Nullable
    public LzyPortalGrpc.LzyPortalBlockingStub getGrpcClient(String executionId) {
        String address;
        try {
            address = withRetries(LOG, () -> executionDao.getPortalVmAddress(executionId));
        } catch (Exception e) {
            LOG.error("Cannot obtain portal address { executionId: {}, error: {} } ", executionId, e.getMessage(), e);
            return null;
        }

        return getGrpcClient(executionId, HostAndPort.fromString(address));
    }

    public LzyPortalGrpc.LzyPortalBlockingStub getGrpcClient(String executionId, HostAndPort portalVmAddress) {
        var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
            newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME));
        return newBlockingClient(LzyPortalGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCredentials.get().token());
    }

    public LongRunningServiceGrpc.LongRunningServiceBlockingStub getOperationsGrpcClient(String executionId,
                                                                                         HostAndPort portalVmAddress)
    {
        var grpcChannel = portalChannels.computeIfAbsent(executionId, exId ->
            newGrpcChannel(portalVmAddress, LzyPortalGrpc.SERVICE_NAME));
        return newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(grpcChannel), APP,
            () -> internalUserCredentials.get().token());
    }
}
