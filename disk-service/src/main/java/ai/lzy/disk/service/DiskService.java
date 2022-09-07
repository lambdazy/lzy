package ai.lzy.disk.service;

import ai.lzy.disk.model.grpc.GrpcConverter;
import ai.lzy.disk.model.Disk;
import ai.lzy.disk.manager.DiskManager;
import ai.lzy.v1.disk.LDS;
import ai.lzy.v1.disk.LzyDiskServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskService extends LzyDiskServiceGrpc.LzyDiskServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(DiskService.class);

    private final DiskManager diskManager;

    @Inject
    public DiskService(DiskManager diskManager) {
        this.diskManager = diskManager;
    }

    @Override
    public void createDisk(LDS.CreateDiskRequest request, StreamObserver<LDS.CreateDiskResponse> responseObserver) {
        LOG.debug("Received create request {}", request);
        try {
            Disk disk = diskManager.createDisk(
                request.getUserId(),
                request.getLabel(),
                GrpcConverter.from(request.getType()),
                request.getSizeGb()
            );
            responseObserver.onNext(LDS.CreateDiskResponse.newBuilder()
                .setDisk(GrpcConverter.to(disk))
                .build());
            LOG.info("Received create request done (diskId={})", disk.id());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal argument exception:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asException());
        } catch (Exception e) {
            LOG.error("Internal error:", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void getDisk(LDS.GetDiskRequest request, StreamObserver<LDS.GetDiskResponse> responseObserver) {
        LOG.debug("Received get request {}", request);
        try {
            if (request.getDiskId().isBlank()) {
                String errorMessage = "Illegal argument exception: empty disk id";
                LOG.error(errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }
            Disk disk = diskManager.findDisk(request.getUserId(), request.getDiskId());
            if (disk == null) {
                LOG.error("Disk (diskId={}) not found", request.getDiskId());
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            responseObserver.onNext(LDS.GetDiskResponse.newBuilder()
                .setDisk(GrpcConverter.to(disk))
                .build());
            LOG.info("Received get request done (diskId={})", request);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal argument exception:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asException());
        } catch (Exception e) {
            LOG.error("Internal error:", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void deleteDisk(LDS.DeleteDiskRequest request, StreamObserver<LDS.DeleteDiskResponse> responseObserver) {
        LOG.debug("Received delete request {}", request);
        try {
            if (request.getDiskId().isBlank()) {
                String errorMessage = "Illegal argument exception: empty disk id";
                LOG.error(errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }
            Disk disk = diskManager.findDisk(request.getUserId(), request.getDiskId());
            if (disk == null) {
                LOG.error("Disk (diskId={}) not found", request.getDiskId());
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            diskManager.deleteDisk(request.getUserId(), request.getDiskId());
            responseObserver.onNext(LDS.DeleteDiskResponse.newBuilder()
                .setDisk(GrpcConverter.to(disk))
                .build());
            LOG.info("Received delete request done (diskId={})", request);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal argument exception:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asException());
        } catch (Exception e) {
            LOG.error("Internal error:", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

}
