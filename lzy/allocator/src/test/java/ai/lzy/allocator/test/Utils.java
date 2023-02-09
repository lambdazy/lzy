package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import yandex.cloud.sdk.Zone;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static ai.lzy.test.GrpcUtils.withGrpcContext;

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
        TimeUtils.waitFlagUp(
            () -> withGrpcContext(
                () -> service.get(
                    LongRunning.GetOperationRequest.newBuilder()
                        .setOperationId(operation.getId())
                        .build())
                    .getDone()),
            timeoutSeconds,
            TimeUnit.SECONDS);

        return withGrpcContext(() -> service.get(
            LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(operation.getId())
                .build()));
    }

    public static String extractSessionId(Operation createSessionOp) {
        try {
            var sid = createSessionOp.getResponse().unpack(VmAllocatorApi.CreateSessionResponse.class).getSessionId();
            Assert.assertNotNull(sid);
            Assert.assertFalse(sid.isBlank());
            return sid;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
