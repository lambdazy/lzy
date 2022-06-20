package ai.lzy.disk.client;

import ai.lzy.priv.v1.LDS;
import ai.lzy.priv.v1.LED;
import ai.lzy.priv.v1.LzyDiskServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskStubClient implements DiskClient {

    private static final Logger LOG = LogManager.getLogger(DiskStubClient.class);

    @Override
    public LED.Disk createDisk(@Nullable String label) {
        LOG.debug("Stubbed create disk request");
        if (label == null) {
            label = "disk";
        }
        return LED.Disk.newBuilder()
            .setDiskId(label + "-disk-id")
            .setType(LED.DiskType.S3_STORAGE)
            .build();
    }

    @Override
    public LED.Disk getDisk(String diskId) {
        LOG.debug("Stubbed get disk request");
        return LED.Disk.newBuilder()
            .setDiskId(diskId)
            .setType(LED.DiskType.S3_STORAGE)
            .build();
    }

    @Override
    public LED.Disk deleteDisk(String diskId) {
        LOG.debug("Stubbed get delete request");
        return LED.Disk.newBuilder()
            .setDiskId(diskId)
            .setType(LED.DiskType.S3_STORAGE)
            .build();
    }
}
