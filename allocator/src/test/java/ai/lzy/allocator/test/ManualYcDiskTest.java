package ai.lzy.allocator.test;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.disk.cloudspecific.yc.YcDiskManager;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.util.auth.YcCredentials;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import yandex.cloud.sdk.Zone;

@Ignore
public class ManualYcDiskTest {
    final YcCredentials credentials = loadCredsFrom();
    final DiskManager diskManager = new YcDiskManager(credentials, Duration.ofMinutes(5));

    private static YcCredentials loadCredsFrom() {
        final String serviceAccountId = "aje7j8kng6mhnh49h51u";
        final String keyId = "aje02l6to3hlal7fucu9";
        final String publicKeyPath = "/tmp/manual_public_key";
        final String privateKeyPath = "/tmp/manual_private_key.rsa";
        final String folderId = "b1gqcrinonvsj9u3th3h";

        try {
            final String publicKey = Files.readString(Paths.get(publicKeyPath));
            final String privateKey = Files.readString(Paths.get(privateKeyPath));
            return new YcCredentials(serviceAccountId, keyId, publicKey, privateKey, folderId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createTest() throws NotFoundException, InternalErrorException, InterruptedException {
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
    public void cloneTest() throws NotFoundException, InternalErrorException, InterruptedException {
        Assert.assertThrows(
            NotFoundException.class,
            () -> diskManager.clone(new Disk("unknown-id", createDiskSpec(3)), createDiskSpec(4))
        );

        final Disk originDisk = diskManager.create(createDiskSpec(3));

        Assert.assertThrows(InternalErrorException.class, () -> diskManager.clone(originDisk, createDiskSpec(2)));

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
