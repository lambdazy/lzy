package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.Operation;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import yandex.cloud.sdk.Zone;

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
            new RandomIdGenerator().generate("test-disk-", 4),
            DiskType.HDD,
            gb,
            zone.getId()
        );
    }

    public static Operation waitOperation(LongRunningServiceBlockingStub service, Operation operation,
                                          long timeoutSeconds)
    {
        return waitOperation(service, operation.getId(), timeoutSeconds);
    }

    public static Operation waitOperation(LongRunningServiceBlockingStub service, String operationId,
                                          long timeoutSeconds)
    {
        TimeUtils.waitFlagUp(
            () -> withGrpcContext(
                () -> service.get(
                        LongRunning.GetOperationRequest.newBuilder()
                            .setOperationId(operationId)
                            .build())
                    .getDone()),
            timeoutSeconds,
            TimeUnit.SECONDS);

        return withGrpcContext(() -> service.get(
            LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(operationId)
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
