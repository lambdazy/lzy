package ai.lzy.allocator.disk.cloudspecific.yc;

import static yandex.cloud.api.compute.v1.DiskServiceOuterClass.CreateDiskRequest;
import static yandex.cloud.api.compute.v1.DiskServiceOuterClass.DeleteDiskRequest;
import static yandex.cloud.api.compute.v1.DiskServiceOuterClass.GetDiskRequest;
import static yandex.cloud.api.compute.v1.SnapshotOuterClass.Snapshot;
import static yandex.cloud.api.compute.v1.SnapshotServiceGrpc.SnapshotServiceBlockingStub;
import static yandex.cloud.api.operation.OperationOuterClass.Operation;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskSpec;
import ai.lzy.allocator.disk.DiskType;
import ai.lzy.allocator.disk.exceptions.InternalErrorException;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import ai.lzy.util.auth.YcCredentials;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;
import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.DiskServiceGrpc.DiskServiceBlockingStub;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.compute.v1.SnapshotServiceOuterClass;
import yandex.cloud.api.compute.v1.SnapshotServiceOuterClass.CreateSnapshotRequest;
import yandex.cloud.api.compute.v1.SnapshotServiceOuterClass.DeleteSnapshotRequest;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.auth.jwt.ServiceAccountKey;
import yandex.cloud.sdk.auth.provider.CredentialProvider;
import yandex.cloud.sdk.utils.OperationUtils;

@Singleton
public class YcDiskManager implements DiskManager {
    private static final Logger LOG = LogManager.getLogger(YcDiskManager.class);
    private static final int GB_MULTIPLIER = 1 << 30;

    private final Duration defaultOperationTimeout;
    private final YcCredentials credentials;
    private final DiskServiceBlockingStub diskService;
    private final SnapshotServiceBlockingStub snapshotService;
    private final OperationServiceBlockingStub operationService;

    public YcDiskManager(YcCredentials credentials, Duration defaultOperationTimeout) {
        this.defaultOperationTimeout = defaultOperationTimeout;
        this.credentials = credentials;
        CredentialProvider provider = Auth.apiKeyBuilder()
            .serviceAccountKey(
                new ServiceAccountKey(
                    credentials.keyId(),
                    credentials.serviceAccountId(),
                    null,
                    "RSA_4096",
                    credentials.publicKey(),
                    credentials.privateKey()))
            .build();
        ServiceFactory factory = ServiceFactory.builder()
            .credentialProvider(provider)
            .requestTimeout(Duration.ofMinutes(1))
            .build();
        diskService = factory.create(DiskServiceBlockingStub.class, DiskServiceGrpc::newBlockingStub);
        snapshotService = factory.create(SnapshotServiceBlockingStub.class, SnapshotServiceGrpc::newBlockingStub);
        operationService = factory.create(OperationServiceBlockingStub.class, OperationServiceGrpc::newBlockingStub);
    }

    @Nullable
    @Override
    public Disk get(String id) {
        LOG.info("Searching disk with id={}", id);
        try {
            final DiskOuterClass.Disk disk = diskService.get(
                GetDiskRequest.newBuilder()
                    .setDiskId(id)
                    .build());
            return new Disk(
                id,
                new DiskSpec(
                    disk.getName(),
                    DiskType.fromYcName(disk.getTypeId()),
                    (int) Math.floor(((float) disk.getSize()) / GB_MULTIPLIER),
                    disk.getZoneId()
                )
            );
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                LOG.warn("Disk with id={} was not found", id);
                return null;
            }
            throw e;
        }
    }

    @Override
    public Disk create(DiskSpec spec) throws InternalErrorException, InterruptedException {
        return create(spec, null);
    }

    private Disk create(DiskSpec spec, @Nullable String snapshotId)
        throws InternalErrorException, InterruptedException {
        LOG.info(
            "Creating disk with name = {} in compute, size = {}Gb, zone = {}",
            spec.name(), spec.sizeGb(), spec.zone()
        );
        var diskRequestBuilder = CreateDiskRequest.newBuilder()
            .setName(spec.name())
            .setFolderId(credentials.folderId())
            .setSize(((long) spec.sizeGb()) * GB_MULTIPLIER)
            .setTypeId(spec.type().toYcName())
            .setZoneId(spec.zone());
        if (snapshotId != null) {
            diskRequestBuilder.setSnapshotId(snapshotId);
        }
        final Operation createDiskOperation = diskService.create(diskRequestBuilder.build());
        try {
            final Operation result = OperationUtils.wait(
                operationService,
                createDiskOperation,
                defaultOperationTimeout
            );
            if (result.hasError()) {
                final String errorMessage = result.getError().getMessage();
                LOG.error("Disk {} creation failed, error={}", spec.name(), errorMessage);
                throw new InternalErrorException(errorMessage);
            }
            final String diskId = result.getResponse().unpack(DiskOuterClass.Disk.class).getId();
            LOG.info("Disk {} was created", spec.name());
            return new Disk(diskId, spec);
        } catch (InvalidProtocolBufferException e) {
            throw new InternalErrorException("Failed via createDisk name=" + spec.name(), e);
        }
    }

    @Override
    public Disk clone(Disk disk, DiskSpec cloneDiskSpec)
        throws NotFoundException, InternalErrorException, InterruptedException {

        LOG.info("Clone disk {}; clone name={} size={}Gb zone={}",
            disk.spec().name(), cloneDiskSpec.name(), cloneDiskSpec.sizeGb(), cloneDiskSpec.zone());
        if (cloneDiskSpec.sizeGb() < disk.spec().sizeGb()) {
            throw new InternalErrorException("Cannot decrease size during clone");
        }

        try {
            LOG.info("Creating snapshot of disk {}", disk.id());
            final CreateSnapshotRequest createSnapshotRequest = CreateSnapshotRequest.newBuilder()
                .setFolderId(credentials.folderId())
                .setDiskId(disk.id())
                .build();
            final Operation snapshotCreateOperation = OperationUtils.wait(
                operationService,
                snapshotService.create(createSnapshotRequest),
                defaultOperationTimeout
            );
            if (snapshotCreateOperation.hasError()) {
                throw new InternalErrorException(
                    "Failed to create snapshot: " + snapshotCreateOperation.getError().getMessage() + ", request id "
                    + snapshotCreateOperation.getId());
            }
            final Snapshot snapshot = snapshotService.get(
                SnapshotServiceOuterClass.GetSnapshotRequest.newBuilder()
                    .setSnapshotId(
                        snapshotCreateOperation.getMetadata()
                        .unpack(SnapshotServiceOuterClass.CreateSnapshotMetadata.class)
                        .getSnapshotId())
                    .build()
            );
            LOG.info("Created snapshot of disk {}, snapshot id = {}", disk.spec().name(), snapshot.getId());

            LOG.info("Creating disk clone with name {} from snapshot {}", cloneDiskSpec.name(), snapshot.getId());
            final Disk clonedDisk = create(cloneDiskSpec, snapshot.getId());

            LOG.info("Deleting snapshot {}", snapshot.getId());
            final Operation deleteSnapshotOperation = OperationUtils.wait(
                operationService,
                snapshotService.delete(
                    DeleteSnapshotRequest.newBuilder()
                        .setSnapshotId(snapshot.getId())
                    .build()),
                defaultOperationTimeout
            );
            if (deleteSnapshotOperation.hasError()) {
                throw new InternalErrorException(
                    "Failed to delete snapshot: " + deleteSnapshotOperation.getError().getMessage() + ", request id "
                        + deleteSnapshotOperation.getId());
            }
            LOG.info("Deleted snapshot {}", snapshot.getId());

            return clonedDisk;
        } catch (InvalidProtocolBufferException e) {
            throw new InternalErrorException(e);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
                throw new NotFoundException();
            }
            throw e;
        }
    }

    @Override
    public void delete(Disk disk) throws NotFoundException, InterruptedException {
        final String diskId = disk.id();
        LOG.info("Deleting disk with id {}", diskId);
        final DeleteDiskRequest deleteDiskRequest = DeleteDiskRequest.newBuilder()
            .setDiskId(diskId)
            .build();
        final Operation delete = OperationUtils.wait(
            operationService,
            diskService.delete(deleteDiskRequest),
            defaultOperationTimeout
        );
        if (delete.hasError()) {
            LOG.error("Failed to delete disk with id {}", diskId);
            if (delete.getError().getCode() == Status.NOT_FOUND.getCode().value()) {
                throw new NotFoundException();
            }
            throw new RuntimeException(delete.getError().getMessage());
        }
        LOG.info("Disk with id={} deleted", diskId);
    }
}
