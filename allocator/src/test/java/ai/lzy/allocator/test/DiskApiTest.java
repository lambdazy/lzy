package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.impl.AllocatorDataSource;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.DiskApi;
import ai.lzy.v1.DiskServiceApi;
import ai.lzy.v1.DiskServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static ai.lzy.allocator.test.Utils.waitOperation;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class DiskApiTest extends BaseTestWithIam {
    private static final int DEFAULT_TIMEOUT_SEC = 300;

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private AllocatorMain allocatorApp;
    private LongRunningServiceGrpc.LongRunningServiceBlockingStub operations;
    private DiskServiceGrpc.DiskServiceBlockingStub diskService;
    private DiskManager diskManager;
    private ManagedChannel channel;

    private final DiskSpec defaultDiskSpec = new DiskSpec("disk", DiskType.HDD, 3, "ru-central1-a");
    private final String defaultUserName = "user-id";

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        final var props = DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo());
        context = ApplicationContext.run(props);

        allocatorApp = context.getBean(AllocatorMain.class);
        allocatorApp.start();

        diskManager = context.getBean(DiskManager.class);

        final var config = context.getBean(ServiceConfig.class);
        channel = newGrpcChannel(config.getAddress(), LongRunningServiceGrpc.SERVICE_NAME,
            DiskServiceGrpc.SERVICE_NAME);

        final var credentials = config.getIam().createRenewableToken();
        operations = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());
        diskService = newBlockingClient(DiskServiceGrpc.newBlockingStub(channel), "Test",
            () -> credentials.get().token());
    }

    @After
    public void after() {
        allocatorApp.stop();
        try {
            allocatorApp.awaitTermination();
        } catch (InterruptedException e) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignored
        }

        DatabaseTestUtils.cleanup(context.getBean(AllocatorDataSource.class));

        context.stop();
        super.after();
    }

    @Test
    public void createDeleteTest() throws InvalidProtocolBufferException {
        var createDiskOperation =
            diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setDiskSpec(defaultDiskSpec.toProto())
                .build());

        createDiskOperation = waitOperation(operations, createDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertFalse(createDiskOperation.hasError());
        Assert.assertTrue(createDiskOperation.hasResponse());

        Assert.assertEquals(defaultUserName, createDiskOperation.getCreatedBy());
        final var createDiskResponse =
            createDiskOperation.getResponse().unpack(DiskServiceApi.CreateDiskResponse.class);
        final DiskApi.Disk disk = createDiskResponse.getDisk();
        Assert.assertEquals(defaultDiskSpec, DiskSpec.fromProto(disk.getSpec()));
        Assert.assertEquals(defaultUserName, disk.getOwner());

        Assert.assertNotNull(diskManager.get(disk.getDiskId()));

        deleteDisk(disk);
        Assert.assertTrue(waitDiskDeletion(disk));
    }

    @Test
    public void createExistingDeleteTest() throws InvalidProtocolBufferException {
        final Disk disk = diskManager.create(defaultDiskSpec, new DiskMeta(defaultUserName));
        var createDiskOperation =
            diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setExistingDisk(DiskServiceApi.CreateDiskRequest.ExistingDisk.newBuilder()
                    .setDiskId(disk.id())
                    .build())
                .build());

        createDiskOperation = waitOperation(operations, createDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertFalse(createDiskOperation.hasError());
        Assert.assertTrue(createDiskOperation.hasResponse());

        Assert.assertEquals(defaultUserName, createDiskOperation.getCreatedBy());
        final var createDiskResponse =
            createDiskOperation.getResponse().unpack(DiskServiceApi.CreateDiskResponse.class);
        final DiskApi.Disk existingDisk = createDiskResponse.getDisk();
        Assert.assertEquals(disk, Disk.fromProto(existingDisk));

        Assert.assertNotNull(diskManager.get(existingDisk.getDiskId()));

        deleteDisk(existingDisk);
        Assert.assertTrue(waitDiskDeletion(existingDisk));
    }

    @Test
    public void createExistingWithBadDiskIdTest() {
        var createDiskOperation = diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
            .setUserId(defaultUserName)
            .setExistingDisk(DiskServiceApi.CreateDiskRequest.ExistingDisk.newBuilder()
                .setDiskId("unknown-disk-id")
                .build())
            .build()
        );

        createDiskOperation = waitOperation(operations, createDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertFalse(createDiskOperation.hasResponse());
        Assert.assertTrue(createDiskOperation.hasError());
        Assert.assertEquals(Status.NOT_FOUND.getCode().value(), createDiskOperation.getError().getCode());
    }

    @Test
    public void cloneDiskTest() throws InvalidProtocolBufferException {
        final DiskApi.Disk disk = createDefaultDisk();

        final DiskSpec clonedDiskSpec = new DiskSpec("clonedDiskName", DiskType.HDD, 4, "ru-central1-a");
        final String newUserId = "new_user_id";
        var cloneDiskOperation =
            diskService.cloneDisk(DiskServiceApi.CloneDiskRequest.newBuilder()
                .setUserId(newUserId)
                .setDiskId(disk.getDiskId())
                .setNewDiskSpec(clonedDiskSpec.toProto())
                .build());
        cloneDiskOperation = waitOperation(operations, cloneDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertEquals(newUserId, cloneDiskOperation.getCreatedBy());
        Assert.assertFalse(cloneDiskOperation.hasError());
        Assert.assertTrue(cloneDiskOperation.hasResponse());

        final var cloneDiskResponse =
            cloneDiskOperation.getResponse().unpack(DiskServiceApi.CloneDiskResponse.class);
        final DiskApi.Disk clonedDisk = cloneDiskResponse.getDisk();

        Assert.assertEquals(clonedDiskSpec, DiskSpec.fromProto(clonedDisk.getSpec()));
        Assert.assertEquals(newUserId, clonedDisk.getOwner());

        Assert.assertNotNull(diskManager.get(disk.getDiskId()));
        Assert.assertNotNull(diskManager.get(clonedDisk.getDiskId()));

        deleteDisk(disk);
        deleteDisk(clonedDisk);

        Assert.assertTrue(waitDiskDeletion(disk));
        Assert.assertTrue(waitDiskDeletion(clonedDisk));
    }

    @Test
    public void cloneWithDowngradeOfDiskSizeTest() throws InvalidProtocolBufferException {
        final DiskApi.Disk disk = createDefaultDisk();
        final DiskSpec clonedDiskSpec = new DiskSpec("clonedDiskName", DiskType.HDD, 1, "ru-central1-a");

        var cloneDiskOperation =
            diskService.cloneDisk(DiskServiceApi.CloneDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setDiskId(disk.getDiskId())
                .setNewDiskSpec(clonedDiskSpec.toProto())
                .build());
        cloneDiskOperation = waitOperation(operations, cloneDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertTrue(cloneDiskOperation.hasError());
        Assert.assertFalse(cloneDiskOperation.hasResponse());
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode().value(), cloneDiskOperation.getError().getCode());

        Assert.assertNotNull(diskManager.get(disk.getDiskId()));
        deleteDisk(disk);
        Assert.assertTrue(waitDiskDeletion(disk));
    }

    @Test
    public void cloneNonExistingDiskTest() {
        final DiskSpec clonedDiskSpec = new DiskSpec("clonedDiskName", DiskType.HDD, 1, "ru-central1-a");

        var cloneDiskOperation =
            diskService.cloneDisk(DiskServiceApi.CloneDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setDiskId("unknown-disk-id")
                .setNewDiskSpec(clonedDiskSpec.toProto())
                .build());
        cloneDiskOperation = waitOperation(operations, cloneDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertTrue(cloneDiskOperation.hasError());
        Assert.assertFalse(cloneDiskOperation.hasResponse());
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode().value(), cloneDiskOperation.getError().getCode());
    }

    @Test
    public void deleteNonExistingDiskTest() {
        try {
            //noinspection ResultOfMethodCallIgnored
            diskService.deleteDisk(DiskServiceApi.DeleteDiskRequest.newBuilder()
                .setDiskId("unknown-disk-id")
                .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.NOT_FOUND.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void deleteOutsideOfDiskServiceCloneTest() throws InvalidProtocolBufferException, NotFoundException {
        final DiskApi.Disk disk = createDefaultDisk();
        diskManager.delete(disk.getDiskId());

        final DiskSpec clonedDiskSpec = new DiskSpec("clonedDiskName", DiskType.HDD, 1, "ru-central1-a");

        var cloneDiskOperation =
            diskService.cloneDisk(DiskServiceApi.CloneDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setDiskId(disk.getDiskId())
                .setNewDiskSpec(clonedDiskSpec.toProto())
                .build());
        cloneDiskOperation = waitOperation(operations, cloneDiskOperation, DEFAULT_TIMEOUT_SEC);
        Assert.assertTrue(cloneDiskOperation.hasError());
        Assert.assertFalse(cloneDiskOperation.hasResponse());
        Assert.assertEquals(Status.DATA_LOSS.getCode().value(), cloneDiskOperation.getError().getCode());
    }

    @Test
    public void deleteOutsideOfDiskServiceDeleteTest() throws NotFoundException, InvalidProtocolBufferException {
        final DiskApi.Disk disk = createDefaultDisk();
        diskManager.delete(disk.getDiskId());

        try {
            //noinspection ResultOfMethodCallIgnored
            diskService.deleteDisk(DiskServiceApi.DeleteDiskRequest.newBuilder()
                .setDiskId(disk.getDiskId()).build());
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.DATA_LOSS.getCode(), e.getStatus().getCode());
        }
    }

    private void deleteDisk(DiskApi.Disk disk) {
        //noinspection ResultOfMethodCallIgnored
        diskService.deleteDisk(DiskServiceApi.DeleteDiskRequest.newBuilder()
            .setDiskId(disk.getDiskId())
            .build());
    }

    private boolean waitDiskDeletion(DiskApi.Disk disk) {
        return TimeUtils.waitFlagUp(
            () -> diskManager.get(disk.getDiskId()) == null,
            5, TimeUnit.SECONDS);
    }

    @NotNull
    private DiskApi.Disk createDefaultDisk() throws InvalidProtocolBufferException {
        var createDiskOperation =
            diskService.createDisk(DiskServiceApi.CreateDiskRequest.newBuilder()
                .setUserId(defaultUserName)
                .setDiskSpec(defaultDiskSpec.toProto())
                .build());

        createDiskOperation = waitOperation(operations, createDiskOperation, DEFAULT_TIMEOUT_SEC);
        final var createDiskResponse =
            createDiskOperation.getResponse().unpack(DiskServiceApi.CreateDiskResponse.class);
        return createDiskResponse.getDisk();
    }
}
