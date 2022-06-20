package ai.lzy.disk.service;

import ai.lzy.common.GrpcConverter;
import ai.lzy.disk.Disk;
import ai.lzy.disk.manager.DiskManager;
import ai.lzy.priv.v1.LDS;
import ai.lzy.priv.v1.LzyDiskServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MockedDiskService extends LzyDiskServiceGrpc.LzyDiskServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(MockedDiskService.class);

    private final Disk diskStub;

    public MockedDiskService(Disk diskStub) {
        this.diskStub = diskStub;
    }

    @Override
    public void create(LDS.CreateDiskRequest request, StreamObserver<LDS.CreateDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::create {}", request);
        responseObserver.onNext(LDS.CreateDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void get(LDS.GetDiskRequest request, StreamObserver<LDS.GetDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::get {}", request);
        responseObserver.onNext(LDS.GetDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void delete(LDS.DeleteDiskRequest request, StreamObserver<LDS.DeleteDiskResponse> responseObserver) {
        LOG.debug("Mocking DiskService::delete {}", request);
        responseObserver.onNext(LDS.DeleteDiskResponse.newBuilder()
            .setDisk(GrpcConverter.to(diskStub))
            .build());
        responseObserver.onCompleted();
    }

}
