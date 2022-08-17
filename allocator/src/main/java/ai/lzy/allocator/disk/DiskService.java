package ai.lzy.allocator.disk;

import ai.lzy.v1.OperationService;
import ai.lzy.v1.allocator.DiskServiceApi;
import ai.lzy.v1.allocator.DiskServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DiskService extends DiskServiceGrpc.DiskServiceImplBase {
    @Override
    public void createDisk(DiskServiceApi.CreateDiskRequest request,
                           StreamObserver<OperationService.Operation> responseObserver) {
        super.createDisk(request, responseObserver);
    }

    @Override
    public void cloneDisk(DiskServiceApi.CloneDiskRequest request,
                          StreamObserver<OperationService.Operation> responseObserver) {
        super.cloneDisk(request, responseObserver);
    }

    @Override
    public void deleteDisk(DiskServiceApi.DeleteDiskRequest request,
                           StreamObserver<OperationService.Operation> responseObserver) {
        super.deleteDisk(request, responseObserver);
    }
}
