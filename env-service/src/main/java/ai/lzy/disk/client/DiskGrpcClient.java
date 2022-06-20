package ai.lzy.disk.client;

import ai.lzy.priv.v1.LDS;
import ai.lzy.priv.v1.LED;
import ai.lzy.priv.v1.LzyDiskServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Inject;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskGrpcClient implements DiskClient {

    private static final Logger LOG = LogManager.getLogger(DiskGrpcClient.class);

    private final LzyDiskServiceGrpc.LzyDiskServiceBlockingStub diskService;

    public DiskGrpcClient(Channel channel) {
        this.diskService = LzyDiskServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public LED.Disk createDisk(@Nullable String label) {
        if (label == null) {
            label = "disk";
        }
        final LED.DiskType diskType = LED.DiskType.S3_STORAGE;
        final LDS.CreateDiskRequest request = LDS.CreateDiskRequest.newBuilder()
            .setLabel(label)
            .setDiskType(diskType)
            .build();
        LOG.debug("Send create disk request {}", request);
        final LDS.CreateDiskResponse response;
        try {
            response = diskService.create(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to create disk: {}", e.getStatus().toString(), e);
            throw new RuntimeException(e.getCause());
        }
        return response.getDisk();
    }

    @Override
    public LED.Disk getDisk(String diskId) {
        final LDS.GetDiskRequest request = LDS.GetDiskRequest.newBuilder()
            .setDiskId(diskId)
            .build();
        LOG.debug("Send get disk request {}", request);
        final LDS.GetDiskResponse response;
        try {
            response = diskService.get(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to get disk: {}", e.getStatus().toString(), e);
            throw new RuntimeException(e.getCause());
        }
        return response.getDisk();
    }

    @Override
    public LED.Disk deleteDisk(String diskId) {
        final LDS.DeleteDiskRequest request = LDS.DeleteDiskRequest.newBuilder()
            .setDiskId(diskId)
            .build();
        LOG.debug("Send delete disk request {}", request);
        final LDS.DeleteDiskResponse response;
        try {
            response = diskService.delete(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to delete disk: {}", e.getStatus().toString(), e);
            throw new RuntimeException(e.getCause());
        }
        return response.getDisk();
    }
}
