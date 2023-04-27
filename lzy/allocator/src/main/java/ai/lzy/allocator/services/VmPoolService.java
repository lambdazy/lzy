package ai.lzy.allocator.services;

import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.v1.VmPoolServiceGrpc;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.v1.VmPoolServiceApi.GetVmPoolsRequest;
import static ai.lzy.v1.VmPoolServiceApi.VmPools;

@Singleton
public class VmPoolService extends VmPoolServiceGrpc.VmPoolServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(VmPoolService.class);

    private final VmPoolRegistry registry;

    @Inject
    public VmPoolService(VmPoolRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void getVmPools(GetVmPoolsRequest request, StreamObserver<VmPools> response) {
        var pools = VmPools.newBuilder();

        if (request.getWithSystemPools()) {
            registry.getSystemVmPools().values().forEach(pool -> pools.addSystemPools(pool.toProto()));
        }

        if (request.getWithUserPools()) {
            registry.getUserVmPools().values().forEach(pool -> pools.addUserPools(pool.toProto()));
        }

        response.onNext(pools.build());
        response.onCompleted();
    }
}
