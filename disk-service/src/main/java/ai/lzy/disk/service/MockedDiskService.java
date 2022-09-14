package ai.lzy.disk.service;

import ai.lzy.disk.model.grpc.GrpcConverter;
import ai.lzy.disk.model.Disk;
import ai.lzy.v1.disk.LDS;
import ai.lzy.v1.disk.LzyDiskServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MockedDiskService extends LzyDiskServiceGrpc.LzyDiskServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(MockedDiskService.class);

    private final Disk diskStub;

    public MockedDiskService(Disk diskStub) {
        this.diskStub = diskStub;
    }

    @Override
    public void createDisk(LDS.CreateDiskRequest request, StreamObserver<LDS.CreateDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::create {}", request);
        responseObserver.onNext(LDS.CreateDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getDisk(LDS.GetDiskRequest request, StreamObserver<LDS.GetDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::get {}", request);
        responseObserver.onNext(LDS.GetDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteDisk(LDS.DeleteDiskRequest request, StreamObserver<LDS.DeleteDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::delete {}", request);
        responseObserver.onNext(LDS.DeleteDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

}
