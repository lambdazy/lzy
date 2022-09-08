package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.OperationService;
import ai.lzy.v1.OperationServiceApiGrpc;
import ai.lzy.v1.OperationService.Operation;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import yandex.cloud.sdk.Zone;

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

    public static Operation waitOperation(
        OperationServiceApiGrpc.OperationServiceApiBlockingStub operationService,
        Operation operation,
        long timeoutSeconds)
    {
        TimeUtils.waitFlagUp(() -> {
            final OperationService.Operation op = operationService.get(
                OperationService.GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
            return op.getDone();
        }, timeoutSeconds, TimeUnit.SECONDS);
        return operationService.get(
            OperationService.GetOperationRequest.newBuilder().setOperationId(operation.getId()).build());
    }
}
