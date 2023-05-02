package ai.lzy.disk.model.grpc;

import ai.lzy.disk.model.EntityNotFoundException;
import ai.lzy.v1.disk.LD;
import ai.lzy.v1.disk.LDS;
import ai.lzy.v1.disk.LzyDiskServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import jakarta.annotation.Nullable;
import javax.annotation.Nullable;

public class DiskGrpcClient implements DiskClient {

    private static final Logger LOG = LogManager.getLogger(DiskGrpcClient.class);

    private final LzyDiskServiceGrpc.LzyDiskServiceBlockingStub diskService;

    public DiskGrpcClient(Channel channel) {
        this.diskService = LzyDiskServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public LD.Disk createDisk(String userId, String label, LD.DiskType type, @Nullable Integer sizeGb) {
        final LDS.CreateDiskRequest request = LDS.CreateDiskRequest.newBuilder()
            .setUserId(userId)
            .setLabel(label)
            .setType(type)
            .setSizeGb(sizeGb == null ? 0 : sizeGb)
            .build();
        LOG.debug("Send create disk request {}", request);
        final LDS.CreateDiskResponse response;
        try {
            response = diskService.createDisk(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to create disk: {}", e.getStatus().toString(), e);
            switch (e.getStatus().getCode()) {
                case INVALID_ARGUMENT -> throw new IllegalArgumentException(e);
                default -> throw new RuntimeException(e);
            }
        }
        return response.getDisk();
    }

    @Override
    public LD.Disk getDisk(String userId, String diskId) throws EntityNotFoundException {
        final LDS.GetDiskRequest request = LDS.GetDiskRequest.newBuilder()
            .setUserId(userId)
            .setDiskId(diskId)
            .build();
        LOG.debug("Send get disk request {}", request);
        final LDS.GetDiskResponse response;
        try {
            response = diskService.getDisk(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to get disk: {}", e.getStatus().toString(), e);
            switch (e.getStatus().getCode()) {
                case NOT_FOUND -> throw new EntityNotFoundException(e);
                case INVALID_ARGUMENT -> throw new IllegalArgumentException(e);
                default -> throw new RuntimeException(e);
            }
        }
        return response.getDisk();
    }

    @Override
    public LD.Disk deleteDisk(String userId, String diskId) throws EntityNotFoundException {
        final LDS.DeleteDiskRequest request = LDS.DeleteDiskRequest.newBuilder()
            .setUserId(userId)
            .setDiskId(diskId)
            .build();
        LOG.debug("Send delete disk request {}", request);
        final LDS.DeleteDiskResponse response;
        try {
            response = diskService.deleteDisk(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to delete disk: {}", e.getStatus().toString(), e);
            switch (e.getStatus().getCode()) {
                case NOT_FOUND -> throw new EntityNotFoundException(e);
                case INVALID_ARGUMENT -> throw new IllegalArgumentException(e);
                default -> throw new RuntimeException(e);
            }
        }
        return response.getDisk();
    }

    @Override
    public List<LD.Disk> listUserDisks(String userId) {
        final LDS.ListUserDisksRequest request = LDS.ListUserDisksRequest.newBuilder()
            .setUserId(userId)
            .build();
        LOG.debug("Send list user disks request {}", request);
        final LDS.ListUserDisksResponse response;
        try {
            response = diskService.listUserDisks(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to list user disks: {}", e.getStatus().toString(), e);
            switch (e.getStatus().getCode()) {
                case INVALID_ARGUMENT -> throw new IllegalArgumentException(e);
                default -> throw new RuntimeException(e);
            }
        }
        return response.getDiskList();
    }

}
