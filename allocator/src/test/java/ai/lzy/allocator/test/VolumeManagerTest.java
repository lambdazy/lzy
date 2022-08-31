package ai.lzy.allocator.test;

import static ai.lzy.allocator.test.Utils.createTestDiskSpec;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactoryImpl;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.DiskVolumeDescription;
import ai.lzy.allocator.volume.KuberVolumeManager;
import ai.lzy.allocator.volume.Volume;
import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.allocator.volume.VolumeManager;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.jsonwebtoken.lang.Assert;
import io.micronaut.context.ApplicationContext;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class VolumeManagerTest {
    private DiskManager diskManager;
    private VolumeManager volumeManager;

    @Before
    public void before() {
        ApplicationContext context = ApplicationContext.run();
        diskManager = context.getBean(DiskManager.class);
        final ServiceConfig serviceConfig = context.getBean(ServiceConfig.class);
        final ClusterRegistry clusterRegistry = context.getBean(ClusterRegistry.class);
        final String clusterId = serviceConfig.getUserClusters().stream().findFirst().orElse(null);
        if (clusterId == null) {
            throw new RuntimeException("No user cluster was specified for manual test");
        }
        final KubernetesClient client = new KuberClientFactoryImpl().build(clusterRegistry.getCluster(clusterId));
        volumeManager = new KuberVolumeManager(client, diskManager);
    }

    @Test
    public void createVolumeTest() throws NotFoundException {
        final Disk disk = diskManager.create(createTestDiskSpec(3));
        final Volume volume = volumeManager.create(
            new DiskVolumeDescription("some-volume-name", disk.id())
        );
        final VolumeClaim volumeClaim = volumeManager.createClaim(volume);
        Assert.notNull(volumeManager.get(volume.name()));
        Assert.notNull(volumeManager.getClaim(volumeClaim.name()));
        volumeManager.deleteClaim(volumeClaim.name());
        volumeManager.delete(volume.name());
        diskManager.delete(disk.id());
        Assert.isNull(volumeManager.get(volume.name()));
        Assert.isNull(volumeManager.getClaim(volumeClaim.name()));
    }

    @Ignore
    @Test
    public void createVolumePerfTest() throws NotFoundException {
        final int numRounds = 100;
        final int minSizeGb = 8;
        final int maxSizeGb = 32;
        final Random random = new Random(42);
        final int numMeas = 6;
        final int[][] perfResults = new int[numRounds][numMeas];

        for (int i = 0; i < numRounds; i++) {
            final DiskSpec testDiskSpec = createTestDiskSpec(random.nextInt(minSizeGb, maxSizeGb));

            final Instant diskCreation = Instant.now();
            final Disk disk = diskManager.create(testDiskSpec);

            final Instant volumeCreation = Instant.now();
            final Volume volume = volumeManager.create(
                new DiskVolumeDescription("some-volume-name", disk.id())
            );

            final Instant volumeClaimCreation = Instant.now();
            final VolumeClaim volumeClaim = volumeManager.createClaim(volume);

            final Instant volumeClaimDeletion = Instant.now();
            volumeManager.deleteClaim(volumeClaim.name());

            final Instant volumeDeletion = Instant.now();
            volumeManager.delete(volume.name());

            final Instant diskDeletion = Instant.now();
            diskManager.delete(disk.id());
            final Instant end = Instant.now();

            perfResults[i] = new int[] {
                (int) Duration.between(diskCreation, volumeCreation).toMillis(),
                (int) Duration.between(volumeCreation, volumeClaimCreation).toMillis(),
                (int) Duration.between(volumeClaimCreation, volumeClaimDeletion).toMillis(),
                (int) Duration.between(volumeClaimDeletion, volumeDeletion).toMillis(),
                (int) Duration.between(volumeDeletion, diskDeletion).toMillis(),
                (int) Duration.between(diskDeletion, end).toMillis()
            };
        }

        int[] median = new int[numMeas];
        int[] p10 = new int[numMeas];
        int[] p90 = new int[numMeas];
        int[] p95 = new int[numMeas];
        int[] p99 = new int[numMeas];
        int[] p100 = new int[numMeas];
        for (int i = 0; i < numMeas; i++) {
            final int finalI = i;
            Arrays.sort(perfResults, Comparator.comparingInt(l -> l[finalI]));
            median[i] = perfResults[49][i];
            p10[i] = perfResults[9][i];
            p90[i] = perfResults[89][i];
            p95[i] = perfResults[94][i];
            p99[i] = perfResults[98][i];
            p100[i] = perfResults[99][i];
        }

        String[] names = new String[] {
            "diskCreation       ",
            "volumeCreation     ",
            "volumeClaimCreation",
            "volumeClaimDeletion",
            "volumeDeletion     ",
            "diskDeletion       "
        };
        DecimalFormat df = new DecimalFormat("#.####");
        df.setRoundingMode(RoundingMode.CEILING);
        System.out.println("type               : median [ p10  ,  p90 ,  p95 ,  p99 ,  p100] (seconds)");
        for (int i = 0; i < numMeas; i++) {
            System.out.println(names[i] + ": "
                + df.format(median[i] / 1000.)
                + " [" + df.format(p10[i] / 1000.)
                + ", " + df.format(p90[i] / 1000.)
                + ", " + df.format(p95[i] / 1000.)
                + ", " + df.format(p99[i] / 1000.)
                + ", " + df.format(p100[i] / 1000.)
                + "]");
        }
    }
}
