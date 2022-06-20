package ai.lzy.disk;

import ai.lzy.disk.service.DiskService;
import ai.lzy.priv.v1.LDS;
import ai.lzy.priv.v1.LED;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiskServiceTest {

    private ApplicationContext ctx;
    private Context grpcCtx;
    private Context prevGrpcCtx;
    private DiskService diskService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        diskService = ctx.getBean(DiskService.class);
        grpcCtx = Context.current();
        prevGrpcCtx = grpcCtx.attach();
    }

    @After
    public void tearDown() {
        grpcCtx.detach(prevGrpcCtx);
        ctx.stop();
    }

    @Test
    public void createDisk() {
        String diskId = doCreateDisk("test", LED.DiskType.LOCAL_DIR);
        doDeleteDisk(diskId);
    }

    public String doCreateDisk(String label, LED.DiskType diskType) {
        AtomicReference<String> createdDiskId = new AtomicReference<>("null");
        diskService.create(
            LDS.CreateDiskRequest.newBuilder()
                .setLabel(label)
                .setDiskType(diskType)
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LDS.CreateDiskResponse createDiskResponse) {
                    Assert.assertEquals(
                        diskType,
                        createDiskResponse.getDisk().getType()
                    );
                    System.out.println(createDiskResponse.getDisk().getDiskId());
                    createdDiskId.set(createDiskResponse.getDisk().getDiskId());
                }

                @Override
                public void onError(Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                @Override
                public void onCompleted() {

                }
            }
        );
        return createdDiskId.get();
    }

    public String doDeleteDisk(String diskId) {
        AtomicReference<String> deletedDiskId = new AtomicReference<>("null");
        diskService.delete(
            LDS.DeleteDiskRequest.newBuilder()
                .setDiskId(diskId)
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LDS.DeleteDiskResponse deleteDiskResponse) {
                    Assert.assertEquals(
                        diskId,
                        deleteDiskResponse.getDisk().getDiskId()
                    );
                    System.out.println(deleteDiskResponse.getDisk().getDiskId());
                    deletedDiskId.set(deleteDiskResponse.getDisk().getDiskId());
                }

                @Override
                public void onError(Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                @Override
                public void onCompleted() {

                }
            }
        );
        return deletedDiskId.get();
    }

}
