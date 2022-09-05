package ai.lzy.allocator.test;

import static ai.lzy.allocator.test.Utils.createTestDiskSpec;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    public void before() throws IOException {
        var properties = new YamlPropertySourceLoader()
            .read("allocator", new FileInputStream("../allocator/src/main/resources/application-test-manual.yml"));
        context = ApplicationContext.run(PropertySource.of(properties));
        diskManager = context.getBean(DiskManager.class);
    }

    @Test
    public void createTest() throws NotFoundException {
        final Disk disk = diskManager.create(createTestDiskSpec(2));

        final Disk retrievedDisk = diskManager.get(disk.id());
        Assert.assertEquals(disk, retrievedDisk);

        diskManager.delete(disk.id());
        final Disk retrievedSecondTimeDisk = diskManager.get(disk.id());
        Assert.assertNull(retrievedSecondTimeDisk);
    }

    @Test
    public void cloneTest() throws NotFoundException {
        Assert.assertThrows(
            NotFoundException.class,
            () -> diskManager.clone(new Disk("unknown-id", createTestDiskSpec(3)), createTestDiskSpec(4))
        );

        final Disk originDisk = diskManager.create(createTestDiskSpec(3));

        Assert.assertThrows(RuntimeException.class, () -> diskManager.clone(originDisk, createTestDiskSpec(2)));

        final Disk clonedDisk = diskManager.clone(originDisk, createTestDiskSpec(4));
        Assert.assertEquals(originDisk.spec().zone(), clonedDisk.spec().zone());
        Assert.assertEquals(4, clonedDisk.spec().sizeGb());

        final Disk clonedDiskDifferentZone = diskManager.clone(originDisk, createTestDiskSpec(4, Zone.RU_CENTRAL1_B));
        Assert.assertEquals(Zone.RU_CENTRAL1_B.getId(), clonedDiskDifferentZone.spec().zone());

        diskManager.delete(originDisk.id());
        diskManager.delete(clonedDisk.id());
        diskManager.delete(clonedDiskDifferentZone.id());
    }
}
