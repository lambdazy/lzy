package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import yandex.cloud.sdk.Zone;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Utils {
    public static DiskSpec createTestDiskSpec(int gb) {
        return createTestDiskSpec(
            gb,
            Zone.RU_CENTRAL1_A
        );
    }

    public static DiskSpec createTestDiskSpec(int gb, Zone zone) {
        return new DiskSpec(
            "test-disk-" + UUID.randomUUID().toString().substring(0, 4),
            DiskType.HDD,
            gb,
            zone.getId()
        );
    }

    public static Operation waitOperation(LongRunningServiceBlockingStub service, Operation operation,
                                          long timeoutSeconds)
    {
        TimeUtils.waitFlagUp(() -> {
            var op = service.get(
                LongRunning.GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
            return op.getDone();
        }, timeoutSeconds, TimeUnit.SECONDS);
        return service.get(
            LongRunning.GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
    }
}
