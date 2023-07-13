package ai.lzy.allocator.test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactoryImpl;
import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.KuberVolumeManager;
import ai.lzy.common.IdGenerator;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.test.TimeUtils;
import ai.lzy.v1.*;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest.Workload;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import yandex.cloud.sdk.Zone;

@Ignore
public class AllocateWithVolumeTest extends BaseTestWithIam {
    private static final int TIMEOUT_SEC = 300;

    private ApplicationContext context;
    private AllocatorGrpc.AllocatorBlockingStub allocator;
    private AllocatorPrivateGrpc.AllocatorPrivateBlockingStub privateAllocatorBlockingStub;
    private AllocatorMain allocatorApp;
    private ManagedChannel channel;
    private KubernetesClient kuber;
    private DiskManager diskManager;
    private IdGenerator idGenerator;

    @Before
    public void before() throws IOException, InterruptedException {
        super.before();

        var properties = new YamlPropertySourceLoader()
            .read("allocator", new FileInputStream("../allocator/src/main/resources/application-test-manual.yml"));
        context = ApplicationContext.run(PropertySource.of(properties));
        var config = context.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());

        diskManager = context.getBean(DiskManager.class);
        final ServiceConfig serviceConfig = context.getBean(ServiceConfig.class);
        final ClusterRegistry clusterRegistry = context.getBean(ClusterRegistry.class);
        final String clusterId = serviceConfig.getUserClusters().stream().findFirst().orElse(null);
        if (clusterId == null) {
            throw new RuntimeException("No user cluster was specified for manual test");
        }
        kuber = new KuberClientFactoryImpl(new TestCredProvider()).build(clusterRegistry.getCluster(clusterId));

        allocatorApp = context.getBean(AllocatorMain.class);
        allocatorApp.start();

        channel = newGrpcChannel(config.getAddress(), AllocatorGrpc.SERVICE_NAME);

        var credentials = config.getIam().createRenewableToken();
        allocator = newBlockingClient(
            AllocatorGrpc.newBlockingStub(channel), "Test", () -> credentials.get().token());
        privateAllocatorBlockingStub = newBlockingClient(
            AllocatorPrivateGrpc.newBlockingStub(channel), "Test", () -> credentials.get().token());

        idGenerator = context.getBean(IdGenerator.class, Qualifiers.byName("AllocatorIdGenerator"));
    }

    @After
    public void after() {
        allocatorApp.stop(false);
        try {
            allocatorApp.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        }

        context.stop();
        super.after();
    }

    private record ExecResult(
        String stdout,
        String stderr,
        int exitCode
    )
    {
    }

    private ExecResult execInPod(String podName, String cmd) {
        var outputStream = new ByteArrayOutputStream();
        var errorStream = new ByteArrayOutputStream();
        ExecWatch watch = kuber.pods().inNamespace("default").withName(podName)
            .redirectingInput()
            .writingOutput(outputStream)
            .writingError(errorStream)
            .exec("sh", "-c", cmd);
        int exitCode = watch.exitCode().join();
        String execStdout = outputStream.toString(StandardCharsets.UTF_8);
        String execStderr = errorStream.toString(StandardCharsets.UTF_8);
        return new ExecResult(execStdout, execStderr, exitCode);
    }

    private interface ExecPodFunc {
        ExecResult apply(String podName);
    }

    private String runWorkloadWithDisk(List<Workload> workloads, String cmd, List<VolumeApi.Volume> volumes)
        throws InvalidProtocolBufferException
    {
        var execResult = runWorkloadWithDisk(workloads, volumes, (podName) -> execInPod(podName, cmd));
        if (execResult == null || execResult.exitCode() != 0) {
            throw new IllegalStateException("Workload has failed");
        }
        System.err.println(execResult.stderr());
        return execResult.stdout();
    }

    @Nullable
    private ExecResult runWorkloadWithDisk(List<Workload> workloads, List<VolumeApi.Volume> volumes,
                                           ExecPodFunc execPodFunc) throws InvalidProtocolBufferException
    {
        var createSessionOp = allocator.createSession(
            VmAllocatorApi.CreateSessionRequest.newBuilder()
                .setOwner(idGenerator.generate("user-"))
                .setCachePolicy(
                    VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.ZERO)
                        .build())
                .build());
        final String sessionId = Utils.extractSessionId(createSessionOp);
        LongRunning.Operation allocateOperation = allocator.allocate(VmAllocatorApi.AllocateRequest.newBuilder()
            .setSessionId(sessionId)
            .setPoolLabel("s")
            .setZone(Zone.RU_CENTRAL1_A.getId())
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.USER)
            .addAllWorkload(workloads)
            .addAllVolumes(volumes)
            .build());
        final VmAllocatorApi.AllocateMetadata allocateMeta =
            allocateOperation.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);

        if (allocateOperation.hasError()) {
            throw new RuntimeException(allocateOperation.getError().getMessage());
        }

        final String podName = KuberVmAllocator.VM_POD_NAME_PREFIX + allocateMeta.getVmId().toLowerCase(Locale.ROOT);
        final AtomicReference<String> podPhase = new AtomicReference<>();
        TimeUtils.waitFlagUp(
            () -> {
                final Pod pod = kuber.pods().inNamespace("default").withName(podName).get();
                if (pod == null) {
                    return false;
                }
                final String phase = pod.getStatus().getPhase();
                podPhase.set(phase);
                return phase.equals("Running") || phase.equals("Succeeded") || phase.equals("Failed");
            },
            TIMEOUT_SEC, TimeUnit.SECONDS
        );
        ExecResult execResult = null;
        if (!podPhase.get().equals("Failed")) {
            registerVm(allocateMeta.getVmId());
            execResult = execPodFunc.apply(podName);
        }
        //noinspection ResultOfMethodCallIgnored
        allocator.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(allocateMeta.getVmId()).build());
        // noinspection ResultOfMethodCallIgnored
        allocator.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
            .setSessionId(sessionId)
            .build());
        return execResult;
    }

    /*
    @Test
    public void allocateTest() throws InvalidProtocolBufferException, NotFoundException {
        final Disk disk = diskManager.create(createTestDiskSpec(3), new DiskMeta("user-id"));
        final String volumeName = "volume";
        final String mountPath = "/mnt/volume";
        final String filePath = mountPath + "/echo42";
        try {
            final var workload = Workload.newBuilder()
                .setName("workload-name")
                .setImage("alpine:3.14")
                .addAllArgs(List.of("sh", "-c", "tail -f /dev/null"))
                .addVolumeMounts(VolumeApi.Mount.newBuilder()
                    .setVolumeName(volumeName)
                    .setMountPath(mountPath)
                    .setReadOnly(false)
                    .setMountPropagation(VolumeApi.Mount.MountPropagation.NONE)
                    .build())
                .build();
            final var volume = VolumeApi.Volume.newBuilder()
                .setName(volumeName)
                .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder().setDiskId(disk.id()).build())
                .build();
            runWorkloadWithDisk(List.of(workload), "echo 42 > " + filePath, List.of(volume));
            final String execStdout = runWorkloadWithDisk(List.of(workload), "cat " + filePath, List.of(volume));
            Assert.assertEquals("42\n", execStdout);
        } finally {
            waitVolumeDeletion(volumeName);
            diskManager.delete(disk.id());
        }
    }
     */

    private void waitVolumeDeletion(String volumeName) {
        TimeUtils.waitFlagUp(
            () -> {
                final var volumes = kuber.persistentVolumes().list().getItems();
                if (volumes.isEmpty()) {
                    return true;
                }
                for (var volume : volumes) {
                    final String requestedVolumeName = volume.getMetadata().getLabels()
                        .get(KuberVolumeManager.REQUESTED_VOLUME_NAME_LABEL);
                    if (requestedVolumeName.equals(volumeName)) {
                        return false;
                    }
                }
                return true;
            }, TIMEOUT_SEC, TimeUnit.SECONDS
        );
    }

    /*
    @Test
    public void bidirectionalMountTest()
        throws InvalidProtocolBufferException, NotFoundException, ExecutionException, InterruptedException
    {
        final Disk disk = diskManager.create(createTestDiskSpec(3), new DiskMeta("user-id"));
        final String hostDirVolumeName = "mountDir";
        final String diskVolumeName = "volume";
        final String hostDirMountPath = "/mnt";
        final String diskMountPath = "/mnt/volume";
        final String filePath = diskMountPath + "/echo42";
        final String filePath2 = diskMountPath + "/echo43";

        try {
            final var baseWorkload = Workload.newBuilder()
                .setName("workload-name")
                .setImage("alpine:3.14")
                .addAllArgs(List.of("sh", "-c", "tail -f /dev/null"))
                .addVolumeMounts(VolumeApi.Mount.newBuilder()
                    .setVolumeName(hostDirVolumeName)
                    .setMountPath(hostDirMountPath)
                    .setReadOnly(false)
                    .setMountPropagation(VolumeApi.Mount.MountPropagation.BIDIRECTIONAL)
                    .build())
                .build();
            final var hostVolume = VolumeApi.Volume.newBuilder()
                .setName(hostDirVolumeName)
                .setHostPathVolume(VolumeApi.HostPathVolumeType.newBuilder()
                    .setPath(hostDirMountPath)
                    .setHostPathType(VolumeApi.HostPathVolumeType.HostPathType.DIRECTORY)
                )
                .build();

            final CompletableFuture<Boolean> isAdditionalWorkloadStarted = new CompletableFuture<>();
            final CompletableFuture<ExecResult> baseWorkloadResult = new CompletableFuture<>();
            final ExecPodFunc baseFunc = (String podName) -> {
                try {
                    isAdditionalWorkloadStarted.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                final ExecResult execResult = execInPod(podName, "cat " + filePath + "; echo 43 > " + filePath2);
                baseWorkloadResult.complete(execResult);
                return execResult;
            };
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    runWorkloadWithDisk(
                        List.of(baseWorkload),
                        List.of(hostVolume),
                        baseFunc
                    );
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            });

            final ExecPodFunc additionalFunc = (podName) -> {
                execInPod(podName, "echo 42 > " + filePath);
                isAdditionalWorkloadStarted.complete(true);
                try {
                    baseWorkloadResult.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                return execInPod(podName, "cat " + filePath2);
            };
            final var additionalWorkload = Workload.newBuilder()
                .setName("workload-name")
                .setImage("alpine:3.14")
                .addAllArgs(List.of("sh", "-c", "tail -f /dev/null"))
                .addVolumeMounts(VolumeApi.Mount.newBuilder()
                    .setVolumeName(diskVolumeName)
                    .setMountPath(diskMountPath)
                    .setReadOnly(false)
                    .setMountPropagation(VolumeApi.Mount.MountPropagation.BIDIRECTIONAL)
                    .build())
                .addVolumeMounts(VolumeApi.Mount.newBuilder()
                    .setVolumeName(hostDirVolumeName)
                    .setMountPath(hostDirMountPath)
                    .setReadOnly(false)
                    .setMountPropagation(VolumeApi.Mount.MountPropagation.BIDIRECTIONAL)
                    .build())
                .build();
            final var additionalDiskVolume = VolumeApi.Volume.newBuilder()
                .setName(diskVolumeName)
                .setDiskVolume(VolumeApi.DiskVolumeType.newBuilder().setDiskId(disk.id()).build())
                .build();
            final var additionalWorkloadResult = runWorkloadWithDisk(
                List.of(additionalWorkload), List.of(hostVolume, additionalDiskVolume), additionalFunc);

            Assert.assertEquals("42\n", baseWorkloadResult.get().stdout());
            Assert.assertNotNull(additionalWorkloadResult);
            Assert.assertEquals("43\n", additionalWorkloadResult.stdout());
        } finally {
            waitVolumeDeletion(diskVolumeName);
            diskManager.delete(disk.id());
        }
    }
     */

    private void registerVm(String vmId) {
        TimeUtils.waitFlagUp(() -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                privateAllocatorBlockingStub.register(
                    VmAllocatorPrivateApi.RegisterRequest.newBuilder().setVmId(vmId).build());
                return true;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                    return false;
                }
                throw new RuntimeException(e);
            }
        }, TIMEOUT_SEC, TimeUnit.SECONDS);
    }
}
