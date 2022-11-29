package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactoryImpl;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMeta;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.allocator.model.DiskVolumeDescription;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.services.DiskService;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.KuberVolumeManager;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.longrunning.OperationService;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.DiskServiceApi;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import yandex.cloud.sdk.auth.IamToken;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import static ai.lzy.allocator.test.Utils.createTestDiskSpec;

@Ignore
public class VolumeManagerTest {
    private DiskManager diskManager;
    private DiskService diskService;
    private VolumeManager volumeManager;
    private OperationService operations;

    @Before
    public void before() throws IOException {
        var properties = new YamlPropertySourceLoader()
            .read("allocator", new FileInputStream("../allocator/src/main/resources/application-test-manual.yml"));
        ApplicationContext context = ApplicationContext.run(PropertySource.of(properties));
        diskManager = context.getBean(DiskManager.class);
        diskService = context.getBean(DiskService.class);
        operations = context.getBean(OperationService.class);
        final ServiceConfig serviceConfig = context.getBean(ServiceConfig.class);
        final ClusterRegistry clusterRegistry = context.getBean(ClusterRegistry.class);
        final String clusterId = serviceConfig.getUserClusters().stream().findFirst().orElse(null);
        if (clusterId == null) {
            throw new RuntimeException("No user cluster was specified for manual test");
        }
        final KubernetesClient client = new KuberClientFactoryImpl(() -> new IamToken("", Instant.MAX))
            .build(clusterRegistry.getCluster(clusterId));
        volumeManager = new KuberVolumeManager(client);
    }

    @Test
    public void createVolumeTest() throws NotFoundException {
        final Disk disk = createDisk(createTestDiskSpec(3), new DiskMeta("user_id"));

        final Volume volume = volumeManager.create(
            new DiskVolumeDescription("id-1", "some-volume-name", disk.id(), disk.spec().sizeGb())
        );
        final VolumeClaim volumeClaim = volumeManager.createClaim(volume);
        Assert.assertNull(volumeManager.get(volume.name()));
        Assert.assertNull(volumeManager.getClaim(volumeClaim.name()));
        volumeManager.deleteClaim(volumeClaim.name());
        volumeManager.delete(volume.name());

        deleteDisk(disk);

        Assert.assertNull(volumeManager.get(volume.name()));
        Assert.assertNull(volumeManager.getClaim(volumeClaim.name()));
    }

    private void deleteDisk(Disk disk) {
        diskService.deleteDisk(
            DiskServiceApi.DeleteDiskRequest.newBuilder()
                .setDiskId(disk.id())
                .build(),
            new GrpcUtils.SuccessStreamObserver<>() {
                @Override
                public void onNext(LongRunning.Operation value) {
                }

                @Override
                public void onCompleted() {
                }
            }
        );
    }

    private Disk createDisk(DiskSpec spec, DiskMeta meta) {
        final LongRunning.Operation[] op = {null};
        diskService.createDisk(
            DiskServiceApi.CreateDiskRequest.newBuilder()
                .setUserId(meta.user())
                .setDiskSpec(spec.toProto())
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LongRunning.Operation value) {
                    op[0] = value;
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace(System.err);
                }

                @Override
                public void onCompleted() {
                }
            });
        Assert.assertNotNull(op[0]);

        final Disk[] disk = {null};
        while (disk[0] == null) {
            operations.get(
                LongRunning.GetOperationRequest.newBuilder()
                    .setOperationId(op[0].getId())
                    .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(LongRunning.Operation value) {
                        if (value.getDone()) {
                            try {
                                disk[0] = Disk.fromProto(
                                    value.getResponse().unpack(DiskServiceApi.CreateDiskResponse.class).getDisk());
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace(System.err);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
        }

        return disk[0];
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
            final Disk disk = createDisk(testDiskSpec, new DiskMeta("user-id"));

            final Instant volumeCreation = Instant.now();
            final Volume volume = volumeManager.create(
                new DiskVolumeDescription("id-1", "some-volume-name", disk.id(), disk.spec().sizeGb())
            );

            final Instant volumeClaimCreation = Instant.now();
            final VolumeClaim volumeClaim = volumeManager.createClaim(volume);

            final Instant volumeClaimDeletion = Instant.now();
            volumeManager.deleteClaim(volumeClaim.name());

            final Instant volumeDeletion = Instant.now();
            volumeManager.delete(volume.name());

            final Instant diskDeletion = Instant.now();
            deleteDisk(disk);
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
