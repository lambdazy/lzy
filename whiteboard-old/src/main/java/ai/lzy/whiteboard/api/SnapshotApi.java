package ai.lzy.whiteboard.api;

import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.whiteboard.SnapshotRepository;
import ai.lzy.whiteboard.auth.Authenticator;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.GrpcConverter;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.model.snapshot.ExecutionSnapshot;
import ai.lzy.model.snapshot.InputExecutionValue;
import ai.lzy.model.snapshot.Snapshot;
import ai.lzy.model.snapshot.SnapshotEntry;
import ai.lzy.model.snapshot.SnapshotEntryStatus;
import ai.lzy.model.snapshot.SnapshotStatus;
import ai.lzy.model.utils.Permissions;
import ai.lzy.whiteboard.auth.SimpleAuthenticator;
import ai.lzy.whiteboard.config.ServiceConfig;
import ai.lzy.whiteboard.exceptions.SnapshotRepositoryException;
import ai.lzy.v1.IAM.Auth;

import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyServerGrpc;

import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.LzyWhiteboard.AbortCommand;
import ai.lzy.v1.LzyWhiteboard.CommitCommand;
import ai.lzy.v1.LzyWhiteboard.CreateEntryCommand;
import ai.lzy.v1.LzyWhiteboard.CreateSnapshotCommand;
import ai.lzy.v1.LzyWhiteboard.EntryStatusCommand;
import ai.lzy.v1.LzyWhiteboard.EntryStatusResponse;
import ai.lzy.v1.LzyWhiteboard.ExecutionDescription;
import ai.lzy.v1.LzyWhiteboard.FinalizeSnapshotCommand;
import ai.lzy.v1.LzyWhiteboard.LastSnapshotCommand;
import ai.lzy.v1.LzyWhiteboard.OperationStatus;
import ai.lzy.v1.LzyWhiteboard.PrepareCommand;
import ai.lzy.v1.LzyWhiteboard.ResolveExecutionCommand;
import ai.lzy.v1.LzyWhiteboard.ResolveExecutionResponse;
import ai.lzy.v1.LzyWhiteboard.SaveExecutionCommand;
import ai.lzy.v1.LzyWhiteboard.SaveExecutionResponse;
import ai.lzy.v1.SnapshotApiGrpc;

@Singleton
@Requires(property = "service.server-uri")
public class SnapshotApi extends SnapshotApiGrpc.SnapshotApiImplBase {

    private static final Logger LOG = LogManager.getLogger(SnapshotApi.class);
    private final Authenticator auth;
    private final SnapshotRepository repository;
    private final LzyServerGrpc.LzyServerBlockingStub server;

    @Inject
    public SnapshotApi(ServiceConfig serviceConfig, SnapshotRepository repository) {
        URI uri = URI.create(serviceConfig.getServerUri());
        final ManagedChannel serverChannel = ChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(serverChannel);
        auth = new SimpleAuthenticator(server);
        this.repository = repository;
    }

    public static boolean matchInputArgs(ExecutionSnapshot execution,
        List<ResolveExecutionCommand.ArgDescription> inputs) {
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
    public void createSnapshot(CreateSnapshotCommand request,
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
                repository.createFromSnapshot(fromSnapshotId, new Snapshot.Impl(
                    snapshotId,
                    URI.create(request.getAuth().getUser().getUserId()),
                    Date.from(ProtoConverter.fromProto(request.getCreationDateUTC())),
                    request.getWorkflowName(),
                    fromSnapshotId
                ));
            } else {
                repository.create(new Snapshot.Impl(snapshotId, URI.create(request.getAuth().getUser().getUserId()),
                    Date.from(ProtoConverter.fromProto(request.getCreationDateUTC())), request.getWorkflowName(), null));
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

    // говорим, что будет вот такая новая entry
    @Override
    public void prepareToSave(PrepareCommand request,
        StreamObserver<OperationStatus> responseObserver) {
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
                request.getEntry().getDependentEntryIdsList(),
                GrpcConverter.contentTypeFrom(request.getEntry().getType()));
        } catch (SnapshotRepositoryException e) {
            LOG.error(
                "SnapshotApi::prepareToSave: Got exception while preparing to save entry {} to snapshot with id {}: {}",
                request.getEntry().getEntryId(), request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::prepareToSave: Successfully executed prepareToSave command");
        final OperationStatus status = OperationStatus
            .newBuilder()
            .setStatus(OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    // финализация, говорим, что запись в s3 прошла
    @Override
    public void commit(CommitCommand request, StreamObserver<OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::commit: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::commit: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for commit command").asException());
            return;
        }
        try {
            SnapshotEntry entry = resolveEntry(request.getAuth(), request.getSnapshotId(), request.getEntryId());
            repository.commit(entry, request.getEmpty());
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::commit: Got exception while commiting entry {} to snapshot with id {}: {}",
                request.getEntryId(), request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::commit: Successfully executed commit command");
        final OperationStatus status = OperationStatus
            .newBuilder()
            .setStatus(OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void abort(AbortCommand request, StreamObserver<OperationStatus> responseObserver) {
        LOG.info("SnapshotApi::abort: Received request");
        if (!auth.checkPermissions(request.getAuth(), Permissions.WHITEBOARD_ALL)) {
            LOG.error("SnapshotApi::abort: Permission denied");
            responseObserver.onError(
                Status.PERMISSION_DENIED.withDescription("Permission denied for abort command").asException());
            return;
        }
        try {
            SnapshotEntry entry = resolveEntry(request.getAuth(), request.getSnapshotId(), request.getEntryId());
            repository.abort(entry);
        } catch (SnapshotRepositoryException e) {
            LOG.error("SnapshotApi::abort: Got exception while aborting entry {} to snapshot with id {}: {}",
                request.getEntryId(), request.getSnapshotId(), e.getMessage());
            responseObserver.onError(e.statusException());
            return;
        }
        LOG.info("SnapshotApi::abort: Successfully executed abort command");
        final OperationStatus status = OperationStatus
            .newBuilder()
            .setStatus(OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotCommand request, StreamObserver<OperationStatus> responseObserver) {
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
        final OperationStatus status = OperationStatus
            .newBuilder()
            .setStatus(OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void lastSnapshot(LastSnapshotCommand request, StreamObserver<LzyWhiteboard.Snapshot> responseObserver) {
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
    public void entryStatus(EntryStatusCommand request, StreamObserver<EntryStatusResponse> responseObserver) {
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
        EntryStatusResponse.Builder builder = EntryStatusResponse.newBuilder()
            .setSnapshotId(snapshotStatus.get().snapshot().id().toString())
            .setEntryId(entry.entry().id())
            .setStatus(EntryStatusResponse.Status.valueOf(entry.status().name()))
            .setEmpty(entry.empty());
        URI storage = entry.storage();
        if (storage != null) {
            builder.setStorageUri(storage.toString());
        }
        EntryStatusResponse resp = builder.build();
        LOG.info("SnapshotApi::entryStatus: Response entry status {} ", JsonUtils.printRequest(resp));
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void createEntry(CreateEntryCommand request, StreamObserver<OperationStatus> responseObserver) {
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
        final OperationStatus status = OperationStatus
            .newBuilder()
            .setStatus(OperationStatus.Status.OK)
            .build();
        responseObserver.onNext(status);
        responseObserver.onCompleted();
    }

    @Override
    public void saveExecution(SaveExecutionCommand request, StreamObserver<SaveExecutionResponse> responseObserver) {
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
        responseObserver.onNext(SaveExecutionResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void resolveExecution(ResolveExecutionCommand request,
        StreamObserver<ResolveExecutionResponse> responseObserver) {
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

        Stream<ExecutionSnapshot> executions = repository.executionSnapshots(request.getOperationName(),
            request.getSnapshotId());
        List<ExecutionDescription> exec = executions.filter(
                execution -> matchInputArgs(execution, request.getArgsList())
            ).map(GrpcConverter::to)
            .collect(Collectors.toList());
        LOG.info("SnapshotApi::resolveExecution: successfully resolved list of execution descriptions");

        ResolveExecutionResponse resp = ResolveExecutionResponse.newBuilder()
            .addAllExecution(exec).build();
        LOG.info("Resolved executions " + JsonUtils.printRequest(resp));

        responseObserver.onNext(resp);
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

    private SnapshotEntry resolveEntry(Auth auth, String snapshotId, String entryId)
        throws SnapshotRepositoryException {
        Optional<SnapshotStatus> snapshot = resolveSnapshot(auth, snapshotId);
        if (snapshot.isEmpty()) {
            throw new SnapshotRepositoryException(Status.NOT_FOUND
                .withDescription("Snapshot with id " + snapshotId + " not found").asException());
        }
        final Optional<SnapshotEntry> entry = repository
            .resolveEntry(snapshot.get().snapshot(), entryId);
        if (entry.isEmpty()) {
            LOG.error("Could not find snapshot entry with id " + entryId
                + " and snapshot id " + snapshotId);
            throw new SnapshotRepositoryException(
                Status.NOT_FOUND.withDescription(
                        "Could not find snapshot entry with id " + entryId
                            + " and snapshot id " + snapshotId)
                    .asException()
            );
        }
        return entry.get();
    }
}
