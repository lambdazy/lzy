package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.micronaut.context.ApplicationContext;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import yandex.cloud.sdk.Zone;

@Ignore
public class ManualYcDiskTest {
    ApplicationContext context;
    DiskManager diskManager;

    @Before
    public void before() {
        context = ApplicationContext.run();
        diskManager = context.getBean(DiskManager.class);
    }

    @Test
    public void createTest() throws NotFoundException {
        final Disk disk = diskManager.create(createDiskSpec(2));

        final Disk retrievedDisk = diskManager.get(disk.id());
        Assert.assertEquals(disk, retrievedDisk);

        diskManager.delete(disk);
        final Disk retrievedSecondTimeDisk = diskManager.get(disk.id());
        Assert.assertNull(retrievedSecondTimeDisk);
    }

    private DiskSpec createDiskSpec(int gb) {
        return createDiskSpec(
            gb,
            Zone.RU_CENTRAL1_A
        );
    }

    private DiskSpec createDiskSpec(int gb, Zone zone) {
        return new DiskSpec(
            "test-disk-" + UUID.randomUUID().toString().substring(0, 4),
            DiskType.HDD,
            gb,
            zone.getId()
        );
    }


    @Test
    public void cloneTest() throws NotFoundException {
        Assert.assertThrows(
            NotFoundException.class,
            () -> diskManager.clone(new Disk("unknown-id", createDiskSpec(3)), createDiskSpec(4))
        );

        final Disk originDisk = diskManager.create(createDiskSpec(3));

        Assert.assertThrows(RuntimeException.class, () -> diskManager.clone(originDisk, createDiskSpec(2)));

        final Disk clonedDisk = diskManager.clone(originDisk, createDiskSpec(4));
        Assert.assertEquals(originDisk.spec().zone(), clonedDisk.spec().zone());
        Assert.assertEquals(4, clonedDisk.spec().sizeGb());

        final Disk clonedDiskDifferentZone = diskManager.clone(originDisk, createDiskSpec(4, Zone.RU_CENTRAL1_B));
        Assert.assertEquals(Zone.RU_CENTRAL1_B.getId(), clonedDiskDifferentZone.spec().zone());

        diskManager.delete(originDisk);
        diskManager.delete(clonedDisk);
        diskManager.delete(clonedDiskDifferentZone);
    }
}
