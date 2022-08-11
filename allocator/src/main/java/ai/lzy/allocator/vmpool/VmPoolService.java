package ai.lzy.allocator.vmpool;

import ai.lzy.v1.VmPoolServiceGrpc;
import io.grpc.stub.StreamObserver;

import static ai.lzy.v1.VmPoolServiceApi.GetVmPoolsRequest;
import static ai.lzy.v1.VmPoolServiceApi.VmPools;

public class VmPoolService extends VmPoolServiceGrpc.VmPoolServiceImplBase {

    @Override
    public void getVmPools(GetVmPoolsRequest request, StreamObserver<VmPools> response) {
    }
}
