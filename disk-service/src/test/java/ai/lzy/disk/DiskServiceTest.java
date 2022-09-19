package ai.lzy.disk;

import ai.lzy.disk.service.DiskService;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.disk.LD;
import ai.lzy.v1.disk.LDS;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class DiskServiceTest {

    private static final String DISK_LABEL_PREFIX = "test";

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext ctx;
    private Context grpcCtx;
    private Context prevGrpcCtx;
    private DiskService diskService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("disk-service", db.getConnectionInfo()));
        diskService = ctx.getBean(DiskService.class);
        grpcCtx = Context.current();
        prevGrpcCtx = grpcCtx.attach();
    }

    @After
    public void tearDown() throws IOException {
        grpcCtx.detach(prevGrpcCtx);
        ctx.stop();
        FileUtils.deleteDirectory(new File("/tmp/lzy-disk/"));
    }

    @Test
    public void createDisk() {
        String diskId1 = doCreateDisk("1", LD.DiskType.LOCAL_DIR);
        Assert.assertTrue(isFileExist(diskId1));
        String diskId2 = doCreateDisk("2", LD.DiskType.LOCAL_DIR);
        Assert.assertTrue(isFileExist(diskId2));
        String diskId3 = doCreateDisk("1", LD.DiskType.LOCAL_DIR);
        Assert.assertTrue(isFileExist(diskId3));
    }

    @Test
    public void deleteDisk() {
        String diskId = doCreateDisk("test", LD.DiskType.LOCAL_DIR);
        Assert.assertTrue(isFileExist(diskId));
        doDeleteDisk(diskId);
        Assert.assertFalse(isFileExist(diskId));
    }

    public String doCreateDisk(String label, LD.DiskType diskType) {
        AtomicReference<String> createdDiskId = new AtomicReference<>("null");
        diskService.createDisk(
            LDS.CreateDiskRequest.newBuilder()
                .setUserId("user")
                .setLabel(DISK_LABEL_PREFIX + "-" + label)
                .setType(diskType)
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LDS.CreateDiskResponse createDiskResponse) {
                    Assert.assertEquals(
                        diskType.getNumber(),
                        createDiskResponse.getDisk().getSpec().getSpecCase().getNumber()
                    );
                    System.out.println(createDiskResponse.getDisk().getId());
                    createdDiskId.set(createDiskResponse.getDisk().getId());
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
        diskService.deleteDisk(
            LDS.DeleteDiskRequest.newBuilder()
                .setUserId("user")
                .setDiskId(diskId)
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LDS.DeleteDiskResponse deleteDiskResponse) {
                    Assert.assertEquals(
                        diskId,
                        deleteDiskResponse.getDisk().getId()
                    );
                    System.out.println(deleteDiskResponse.getDisk().getId());
                    deletedDiskId.set(deleteDiskResponse.getDisk().getId());
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

    private boolean isFileExist(String diskId) {
        return FileUtils.listFilesAndDirs(new File("/tmp/lzy-disk/"), TrueFileFilter.TRUE, null)
            .stream().anyMatch(file -> file.getName().contains(diskId));
    }
}
