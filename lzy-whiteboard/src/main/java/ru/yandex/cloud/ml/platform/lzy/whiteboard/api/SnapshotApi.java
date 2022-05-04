package ru.yandex.cloud.ml.platform.lzy.whiteboard.api;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.ExecutionSnapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.InputExecutionValue;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.Snapshot;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntry;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotEntryStatus;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Permissions;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotRepository;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.auth.SimpleAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.config.ServerConfig;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions.SnapshotRepositoryException;
import yandex.cloud.priv.datasphere.v2.lzy.IAM.Auth;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

@Singleton
@Requires(property = "server.uri")
public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {

    private static final Logger LOG = LogManager.getLogger(SnapshotApi.class);
    private final Authenticator auth;
    private final SnapshotRepository repository;
    private final LzyServerGrpc.LzyServerBlockingStub server;

    @Inject
    public SnapshotApi(ServerConfig serverConfig, SnapshotRepository repository) {
        URI uri = URI.create(serverConfig.getUri());
        final ManagedChannel serverChannel = ChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);
        auth = new SimpleAuthenticator(server);
        this.repository = repository;
    }

    public static boolean matchInputArgs(
        ExecutionSnapshot execution,
        List<LzyWhiteboard.ResolveExecutionCommand.ArgDescription> inputs
    ) {
        HashMap<String, InputExecutionValue> argsMap = new HashMap<>();
        execution.inputs().forEach(arg -> argsMap.put(arg.name(), arg));
        final boolean allMatch = inputs.stream()
            .allMatch(arg -> {
                if (!argsMap.containsKey(arg.getName())) {
                    return false;
                }
                InputExecutionValue executionArg = argsMap.get(arg.getName());
                if (arg.hasEntryId()) {
                    return executionArg.entryId().equals(arg.getEntryId());
                }
                return executionArg.hash().equals(arg.getHash());
            });
        return allMatch && inputs.size() == argsMap.size();
    }

    @Override
    public void createSnapshot(LzyWhiteboard.CreateSnapshotCommand request,
        StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
        LOG.info("SnapshotApi::createSnapshot: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::createSnapshot: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied to create snapshot").asException());
            return;
        }
        if (!request.hasCreationDateUTC()) {
            LOG.error("Snapshot creation date is not provided");
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Snapshot creation date must be provided").asException());
            return;
        }
        URI snapshotId = URI.create("snapshot://" + UUID.randomUUID().toString());
        String fromSnapshotId = request.getFromSnapshot();
        try {
            if (!Objects.equals(fromSnapshotId, "")) {
                final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), fromSnapshotId);
                if (snapshotStatus.isEmpty()) {
                    responseObserver.onError(
                        Status.NOT_FOUND.withDescription("Could not find snapshot with id " + fromSnapshotId)
                            .asException());
                    return;
                }
                if (!Objects.equals(snapshotStatus.get().snapshot().workflowName(), request.getWorkflowName())) {
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                            "Parent snapshot workflow name " + snapshotStatus.get().snapshot().workflowName()
                                + " is different from child snapshot workflow name " + request.getWorkflowName())
                        .asException());
                    return;
                }
                repository.createFromSnapshot(fromSnapshotId,
                    new Snapshot.Impl(snapshotId, URI.create(request.getAuth().getUser().getUserId()),
                        GrpcConverter.from(request.getCreationDateUTC()), request.getWorkflowName(), fromSnapshotId));
            } else {
                repository.create(new Snapshot.Impl(snapshotId, URI.create(request.getAuth().getUser().getUserId()),
                    GrpcConverter.from(request.getCreationDateUTC()), request.getWorkflowName(), null));
            }
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::createSnapshot: Got exception while creating snapshot {}", e.getMessage());
            responseObserver.onError(e.statusException());
        }
        LOG.info("SnapshotApi::createSnapshot: Successfully created snapshot with id {}", snapshotId);
        final LzyWhiteboard.Snapshot result = LzyWhiteboard.Snapshot
            .newBuilder()
            .setSnapshotId(snapshotId.toString())
            .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void prepareToSave(LzyWhiteboard.PrepareCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::prepareToSave: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::prepareToSave: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for prepareToSave command").asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        try {
            repository.prepare(GrpcConverter.from(request.getEntry(), snapshotStatus.get().snapshot()),
                request.getEntry().getStorageUri(),
                request.getEntry().getDependentEntryIdsList());
        } catch (SnapshotRepositoryException e) {
            LOG.error(
                "SnapshotApi::prepareToSave: Got exception while preparing to save entry {} to snapshot with id {}: {}",
                request.getEntry().getEntryId(), request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::prepareToSave: Successfully executed prepareToSave command");
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(LzyWhiteboard.CommitCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::commit: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::commit: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for commit command").asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        final Optional<SnapshotEntry> entry = repository
            .resolveEntry(snapshotStatus.get().snapshot(), request.getEntryId());
        if (entry.isEmpty()) {
            LOG.error("SnapshotApi::commit: Could not find snapshot entry with id " + request.getEntryId()
                + " and snapshot id " + request.getSnapshotId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription(
                        "Could not find snapshot entry with id " + request.getEntryId()
                            + " and snapshot id " + request.getSnapshotId())
                    .asException());
            return;
        }
        try {
            repository.commit(entry.get(), request.getEmpty(), request.getErrored());
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::commit: Got exception while commiting entry {} to snapshot with id {}: {}",
                request.getEntryId(), request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::commit: Successfully executed commit command");
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::finalizeSnapshot: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::finalizeSnapshot: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for finalizeSnapshot command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        try {
            repository.finalize(snapshotStatus.get().snapshot());
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::finalizeSnapshot: Got exception while finalizing snapshot with id {}: {}",
                request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::finalizeSnapshot: Successfully executed finalizeSnapshot command");
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void lastSnapshot(LzyWhiteboard.LastSnapshotCommand request,
        StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
        LOG.info("SnapshotApi::lastSnapshot: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::lastSnapshot: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for lastSnapshot command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = repository.lastSnapshot(request.getWorkflowName(),
            request.getAuth().getUser().getUserId());
        final LzyWhiteboard.Snapshot.Builder result = LzyWhiteboard.Snapshot.newBuilder();
        if (snapshotStatus.isPresent()) {
            result.setSnapshotId(snapshotStatus.get().snapshot().id().toString());
            LOG.info("SnapshotApi::lastSnapshot: Resolved last snapshot to {}", snapshotStatus);
        }
        LOG.info("SnapshotApi::lastSnapshot: Successfully executed lastSnapshot command");
        responseObserver.onNext(result.build());
        responseObserver.onCompleted();
    }

    @Override
    public void entryStatus(LzyWhiteboard.EntryStatusCommand request,
        StreamObserver<LzyWhiteboard.EntryStatusResponse> responseObserver) {
        LOG.info("SnapshotApi::entryStatus: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::entryStatus: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for entryStatus command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        Optional<SnapshotEntryStatus> entryOptional =
            repository.resolveEntryStatus(snapshotStatus.get().snapshot(), request.getEntryId());
        if (entryOptional.isEmpty()) {
            LOG.error("SnapshotApi::entryStatus: Entry {} not found", request.getEntryId());
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Entry " + request.getEntryId() + " not found").asException());
            return;
        }
        SnapshotEntryStatus entry = entryOptional.get();
        LzyWhiteboard.EntryStatusResponse.Builder builder = LzyWhiteboard.EntryStatusResponse.newBuilder()
            .setSnapshotId(snapshotStatus.get().snapshot().id().toString())
            .setEntryId(entry.entry().id())
            .setStatus(LzyWhiteboard.EntryStatusResponse.Status.valueOf(entry.status().name()))
            .setEmpty(entry.empty());
        URI storage = entry.storage();
        if (storage != null) {
            builder.setStorageUri(storage.toString());
        }
        LzyWhiteboard.EntryStatusResponse resp = builder.build();
        LOG.info("SnapshotApi::entryStatus: Response entry status {} ", JsonUtils.printRequest(resp));
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void createEntry(LzyWhiteboard.CreateEntryCommand request,
        StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::createEntry: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::createEntry: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for createEntry command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        SnapshotEntry entry;
        try {
            entry = repository.createEntry(snapshotStatus.get().snapshot(), request.getEntryId());
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::createEntry: Got exception while creating entry {}: {}", request.getEntryId(),
                e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::createEntry: Created entry " + entry);
        final LzyWhiteboard.OperationStatus status = LzyWhiteboard.OperationStatus
            .newBuilder()
            .setStatus(LzyWhiteboard.OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void saveExecution(LzyWhiteboard.SaveExecutionCommand request,
        StreamObserver<LzyWhiteboard.SaveExecutionResponse> responseObserver) {
        LOG.info("SnapshotApi::saveExecution: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::saveExecution: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for saveExecution command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus =
            resolveSnapshot(request.getAuth(), request.getDescription().getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription(
                        "Could not find snapshot with id " + request.getDescription().getSnapshotId())
                    .asException());
            return;
        }
        ExecutionSnapshot execution = GrpcConverter.from(request.getDescription());
        repository.saveExecution(execution);
        LOG.info("SnapshotApi::saveExecution: Saved execution {} ", execution);
        responseObserver.onNext(LzyWhiteboard.SaveExecutionResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void resolveExecution(LzyWhiteboard.ResolveExecutionCommand request,
        StreamObserver<LzyWhiteboard.ResolveExecutionResponse> responseObserver) {
        LOG.info("SnapshotApi::resolveExecution: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::resolveExecution: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for resolveExecution command")
                    .asException());
            return;
        }
        final Optional<SnapshotStatus> snapshotStatus = resolveSnapshot(request.getAuth(), request.getSnapshotId());
        if (snapshotStatus.isEmpty()) {
            responseObserver.onError(
                Status.NOT_FOUND.withDescription("Could not find snapshot with id " + request.getSnapshotId())
                    .asException());
            return;
        }
        Stream<ExecutionSnapshot> executions =
            repository.executionSnapshots(request.getOperationName(), request.getSnapshotId());
        List<LzyWhiteboard.ExecutionDescription> exec = executions.filter(
                execution -> matchInputArgs(execution, request.getArgsList())
            ).map(GrpcConverter::to)
            .collect(Collectors.toList());
        LOG.info("SnapshotApi::resolveExecution: successfully resolved list of execution descriptions");

        LzyWhiteboard.ResolveExecutionResponse resp = LzyWhiteboard.ResolveExecutionResponse.newBuilder()
            .addAllExecution(exec).build();

        LOG.info("Resolved executions " + JsonUtils.printRequest(resp));

        responseObserver
            .onNext(resp);

        responseObserver.onCompleted();
    }

    private Optional<String> resolveUser(Auth auth) {
        Lzy.GetUserResponse response = server.getUser(Lzy.GetUserRequest.newBuilder().setAuth(auth).build());
        if (Objects.equals("", response.getUserId())) {
            return Optional.empty();
        }
        return Optional.of(response.getUserId());
    }

    private Optional<SnapshotStatus> resolveSnapshot(Auth auth, String snapshotId) {
        Optional<String> uid = resolveUser(auth);
        Optional<SnapshotStatus> snapshotStatus = repository.resolveSnapshot(URI.create(snapshotId));
        if (snapshotStatus.isEmpty() || uid.isEmpty()
            || !Objects.equals(snapshotStatus.get().snapshot().uid().toString(), uid.get())) {
            LOG.error("SnapshotApi::resolveSnapshot: Snapshot {} not found", snapshotId);
            return Optional.empty();
        }
        return snapshotStatus;
    }
}
