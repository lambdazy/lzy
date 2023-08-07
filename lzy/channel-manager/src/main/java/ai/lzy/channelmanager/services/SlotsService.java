package ai.lzy.channelmanager.services;

import ai.lzy.channelmanager.ActionScheduler;
import ai.lzy.channelmanager.LzyServiceClient;
import ai.lzy.channelmanager.access.IamAccessManager;
import ai.lzy.channelmanager.db.ChannelDao;
import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.db.PeerDao;
import ai.lzy.channelmanager.db.TransferDao;
import ai.lzy.channelmanager.db.TransferDao.State;
import ai.lzy.channelmanager.model.Channel;
import ai.lzy.channelmanager.model.Peer;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LCMS.*;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LC.PeerDescription;
import ai.lzy.v1.common.LC.PeerDescription.SlotPeer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ai.lzy.channelmanager.db.PeerDao.Priority.BACKUP;
import static ai.lzy.channelmanager.model.Peer.Role.CONSUMER;
import static ai.lzy.channelmanager.model.Peer.Role.PRODUCER;
import static ai.lzy.iam.resources.AuthPermission.WORKFLOW_RUN;
import static ai.lzy.util.grpc.GrpcHeaders.getIdempotencyKey;

@Singleton
public class SlotsService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);

    private final PeerDao peerDao;
    private final ChannelDao channelDao;
    private final ChannelManagerDataSource storage;
    private final TransferDao transferDao;
    private final ActionScheduler action;
    private final LzyServiceClient lzyServiceClient;
    private final IamAccessManager accessManager;

    public SlotsService(PeerDao peerDao, ChannelDao channelDao, ChannelManagerDataSource storage,
                        TransferDao transferDao, ActionScheduler action, LzyServiceClient lzyServiceClient,
                        IamAccessManager accessManager)
    {
        this.peerDao = peerDao;
        this.channelDao = channelDao;
        this.storage = storage;
        this.transferDao = transferDao;
        this.action = action;
        this.lzyServiceClient = lzyServiceClient;
        this.accessManager = accessManager;
    }

    @Override
    public void bind(LCMS.BindRequest request, StreamObserver<LCMS.BindResponse> responseObserver) {
        var channelId = request.getChannelId();
        var peerId = request.getPeerId();
        var peerDesc = PeerDescription.newBuilder()
            .setPeerId(peerId)
            .setSlotPeer(SlotPeer.newBuilder()
                .setPeerUrl(request.getPeerUrl())
                .build())
            .build();

        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "Bind");

        var logPrefix = "(Bind: {peerId: %s, channelId: %s, userId: %s, workflowName: %s, execId: %s}): ".formatted(
            peerId, channelId, channel.userId(), channel.workflowName(), channel.executionId()
        );

        var role = switch (request.getRole()) {
            case UNSPECIFIED, UNRECOGNIZED -> {
                LOG.error("{} Cannot determine role", logPrefix);

                throw Status.INVALID_ARGUMENT
                    .withDescription("Role of peer not set or unsupported")
                    .asRuntimeException();
            }
            case CONSUMER -> CONSUMER;
            case PRODUCER -> PRODUCER;
        };

        var idempotencyKey = getIdempotencyKey();
        var requestHash = IdempotencyUtils.md5(request);

        if (role == CONSUMER) {
            var producerAndTransfer = createConsumer(channelId, peerDesc, logPrefix, role, idempotencyKey, requestHash);

            var builder = LCMS.BindResponse.newBuilder();
            if (producerAndTransfer != null) {
                LOG.info("{} Connected to producer(peerId: {})", logPrefix, producerAndTransfer.peer.id());

                builder.setPeer(producerAndTransfer.peer.description());
                builder.setTransferId(producerAndTransfer.transferId);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

            return;
        }

        var producerAndConsumerTransfers = createProducer(channelId, peerId, peerDesc, logPrefix, role, idempotencyKey,
            requestHash);

        // Finding storage consumer
        PeerAndTransfer storageConsumer = null;
        for (var consumer : producerAndConsumerTransfers.transfers) {
            if (consumer.peer().description().hasStoragePeer()) {
                storageConsumer = consumer;
                break;
            }
        }

        var builder = LCMS.BindResponse.newBuilder();
        if (storageConsumer != null) {
            builder.setPeer(storageConsumer.peer.description());
            builder.setTransferId(storageConsumer.transferId);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();

        // Start actions after response is sent
        var transfers = producerAndConsumerTransfers.transfers;

        for (int i = 0, n = transfers.size(); i < n; i++) {
            var peerAndTransfer = transfers.get(i);

            if (!peerAndTransfer.peer().description().hasStoragePeer()) {
                action.runStartTransferAction(peerAndTransfer.transferId, producerAndConsumerTransfers.producer,
                    peerAndTransfer.peer, idempotencyKey + "_" + i);
            }
        }
    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> responseObserver) {
        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "Unbind");

        var logPrefix = "(Unbind: {peerId: %s, channelId: %s, userId: %s, workflowName: %s, execId: %s}): ".formatted(
            request.getPeerId(), request.getChannelId(), channel.userId(), channel.workflowName(), channel.executionId()
        );
        final boolean res;

        try {
            res = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    if (!transferDao.hasPendingOrActiveTransfers(request.getPeerId(), channel.id(), tx)) {
                        peerDao.drop(request.getPeerId(), channel.id(), tx);
                        tx.commit();
                        return true;
                    }
                    return false;
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot unbind slot: ", logPrefix, e);
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription("Cannot unbind slot")
                    .asRuntimeException());
            return;
        }

        if (res) {
            responseObserver.onNext(LCMS.UnbindResponse.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        LOG.error("{} Cannot unbind this slot now, there are some not started transfers", logPrefix);

        responseObserver.onError(Status
            .UNAVAILABLE
            .withDescription("Cannot unbind this slot now, there are some not started transfers")
            .asRuntimeException());
    }

    @Override
    public void transferFailed(TransferFailedRequest request, StreamObserver<TransferFailedResponse> responseObserver) {
        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "TransferFailed");

        var logPrefix = ("(TransmissionFailed: {transferId: %s,  channelId: %s, userId: %s," +
            " workflowName: %s, execId: %s}): ").formatted(request.getTransferId(), request.getChannelId(),
            channel.userId(), channel.workflowName(), channel.executionId()
        );

        var idempotencyKey = getIdempotencyKey();
        var requestHash = IdempotencyUtils.md5(request);

        final PeerAndTransfer resp;

        try {
            resp = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var transfer = transferDao.get(request.getTransferId(), channel.id(), tx);

                    if (transfer == null) {
                        LOG.error("{} Cannot find transfer in db", logPrefix);
                        throw Status.NOT_FOUND
                            .withDescription("Cannot find transfer in db")
                            .asRuntimeException();
                    }

                    if (transfer.state() != State.FAILED && transfer.state() != State.ACTIVE ||
                        transfer.state() == State.FAILED && !Objects.equals(transfer.stateChangeIdk(), idempotencyKey))
                    {
                        LOG.error("{} Transfer is not active", logPrefix);
                        throw Status.FAILED_PRECONDITION
                            .withDescription("Transfer is not active")
                            .asRuntimeException();
                    }

                    if (transfer.state() == State.ACTIVE) {
                        LOG.error("{} Failed with description '{}', current transfer: {}",
                            logPrefix, request.getDescription(), transfer);

                        transferDao.markFailed(request.getTransferId(), channel.id(), request.getDescription(),
                            idempotencyKey, tx);

                        if (transfer.to().description().hasStoragePeer()) {
                            // Failed to send data to storage peer, failing whole channel
                            LOG.error("{} Uploading data to storage {} for channel failed", logPrefix,
                                transfer.to().description().getStoragePeer().getStorageUri());

                            lzyServiceClient.destroyChannelAndWorkflow(channel.id(), "Uploading data to storage failed",
                                idempotencyKey, tx);
                            tx.commit();

                            throw Status.INTERNAL
                                .withDescription("Uploading data to storage failed")
                                .asRuntimeException();
                        }

                        // Trying to find other producer
                        var newPriority = peerDao.decrementPriority(transfer.from().id(), channel.id(), tx);
                        if (newPriority < 0) {
                            LOG.error("{} Count of retries of connecting to peer {} is exceeded." +
                                " It will be not used for other connections", logPrefix, transfer.from().id());
                        }
                    }

                    // Failed to load data from producer, selecting other
                    var newProducer = peerDao.findProducer(transfer.channelId(), tx);
                    if (newProducer == null) {
                        // This can be only if there was only one producer and connection retries count to it exceeded
                        LOG.error("{} No more producers in channel, while there are consumers", logPrefix);

                        lzyServiceClient.destroyChannelAndWorkflow(channel.id(), "No more producers in channel, " +
                            "while there are consumers", idempotencyKey, tx);
                        tx.commit();
                        throw Status.INTERNAL
                            .withDescription("No more producers in channel, while there are consumers")
                            .asRuntimeException();
                    }

                    // Creating new transfer with found producer
                    var newTransferId = transferDao.create(newProducer.id(), transfer.from().id(), channel.id(),
                        State.ACTIVE, idempotencyKey, requestHash, tx);
                    tx.commit();
                    return new PeerAndTransfer(newProducer, newTransferId);
                }
            });
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            throw e;
        } catch (Exception e) {
            LOG.error("{} Cannot process transfer failed: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot process transfer failed")
                .asRuntimeException();
        }

        responseObserver.onNext(TransferFailedResponse.newBuilder()
            .setNewPeer(resp.peer().description())
            .setNewTransferId(resp.transferId())
            .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void transferCompleted(TransferCompletedRequest request,
                                  StreamObserver<TransferCompletedResponse> responseObserver)
    {
        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "TransmissionCompleted");

        var logPrefix = ("(TransmissionCompleted: {transferId: %s,  channelId: %s, userId: %s," +
            " workflowName: %s, execId: %s}): ").formatted(request.getTransferId(),
            request.getChannelId(), channel.userId(), channel.workflowName(), channel.executionId()
        );

        var idempotencyKey = getIdempotencyKey();
        var requestHash = IdempotencyUtils.md5(request);

        try {
            DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var transfer = transferDao.get(request.getTransferId(), channel.id(), tx);

                    if (transfer == null) {
                        LOG.error("{} Cannot find transfer in db", logPrefix);
                        throw Status.NOT_FOUND
                            .withDescription("Cannot find transfer in db")
                            .asRuntimeException();
                    }

                    var actualIdempotencyKey = transfer.stateChangeIdk();
                    if (transfer.state() != State.COMPLETED && transfer.state() != State.ACTIVE ||
                        transfer.state() == State.COMPLETED && !Objects.equals(actualIdempotencyKey, idempotencyKey))
                    {
                        LOG.error("{} Transfer is not active", logPrefix);
                        throw Status.FAILED_PRECONDITION
                            .withDescription("Transfer is not active")
                            .asRuntimeException();
                    }

                    LOG.info("{} Succeeded", logPrefix);
                    transferDao.markCompleted(request.getTransferId(), channel.id(), idempotencyKey, tx);

                    if (transfer.to().description().hasStoragePeer()) {
                        // Data uploaded to storage, we can use it as producer

                        peerDao.drop(transfer.to().id(), channel.id(), tx);
                        peerDao.create(channel.id(), transfer.to().description(), PRODUCER, BACKUP, false,
                            idempotencyKey, requestHash, tx);
                    }

                    tx.commit();
                }
            });
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            throw e;
        } catch (Exception e) {
            LOG.error("{} Cannot process transfer completed: ", logPrefix, e);
            var ex = Status.INTERNAL
                .withDescription("Cannot process transfer completed")
                .asRuntimeException();

            responseObserver.onError(ex);
            throw ex;
        }

        responseObserver.onNext(TransferCompletedResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getChannelsStatus(GetChannelsStatusRequest request,
                                  StreamObserver<GetChannelsStatusResponse> responseObserver)
    {
        final List<ChannelDao.ChannelStatus> channels;
        try {
            channels = DbHelper.withRetries(LOG,
                () -> channelDao.list(request.getExecutionId(), request.getChannelIdsList(), null));
        } catch (Exception e) {
            LOG.error("Cannot find channels in db: ", e);
            throw Status.INTERNAL.asRuntimeException();
        }

        if (channels.size() == 0) {
            responseObserver.onNext(GetChannelsStatusResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        final var authenticationContext = AuthenticationContext.current();
        final String subjId = Objects.requireNonNull(authenticationContext).getSubject().id();

        var channel = channels.get(0).channel();

        var hasAccess = accessManager.checkAccess(subjId, channel.userId(), channel.workflowName(), WORKFLOW_RUN);

        if (!hasAccess) {
            LOG.error("GetChannelsStatus: Permission denied (executionId: {}, userId: {}, subjId: {})",
                channel.executionId(), channel.userId(), subjId);
            throw Status.PERMISSION_DENIED.asRuntimeException();
        }

        var builder = GetChannelsStatusResponse.newBuilder();

        for (var chan : channels) {
            var channelStatus = LC.ChannelStatus.newBuilder()
                .setChannelId(chan.channel().id())
                .setExecutionId(chan.channel().executionId())
                .setScheme(chan.channel().dataScheme())
                .addAllConsumers(chan.consumers())
                .addAllProducers(chan.producers())
                .build();

            builder.addChannels(channelStatus);
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private Channel getChannelAndCheckAccess(String channelId, String callName) {
        final Channel channel;

        try {
            channel = DbHelper.withRetries(LOG, () -> channelDao.get(channelId, null));
        } catch (Exception e) {
            LOG.error("Cannot get channel {}", channelId, e);

            throw Status.INTERNAL.asRuntimeException();
        }

        final var authenticationContext = AuthenticationContext.current();
        final String subjId = Objects.requireNonNull(authenticationContext).getSubject().id();

        var hasAccess = accessManager.checkAccess(subjId, channel.userId(), channel.workflowName(), WORKFLOW_RUN);

        if (!hasAccess) {
            LOG.error("{}: Permission denied (executionId: {}, userId: {}, subjId: {})", callName,
                channel.executionId(), channel.userId(), subjId);
            throw Status.PERMISSION_DENIED.asRuntimeException();
        }
        return channel;
    }

    /**
     * Atomic operation to create producer
     * If there are not connected consumers, then this consumers will be marked as connected
     * and transfers will be created.
     *
     * @return pair of producer and list of consumer transfers
     */
    private ProducerAndConsumerTransfers createProducer(String channelId, String peerId, PeerDescription peerDesc,
                                                        String logPrefix, Peer.Role role, String idempotencyKey,
                                                        String requestHash)
    {
        try {
            return DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var prod = peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, false,
                        idempotencyKey, requestHash, tx);

                    var consumers = peerDao.listConnectedConsumersByRequest(channelId, idempotencyKey, requestHash, tx);
                    if (consumers.isEmpty()) {
                        consumers = peerDao.markConsumersAsConnected(channelId, idempotencyKey, requestHash, tx);
                    }

                    final var transfers = new ArrayList<PeerAndTransfer>();

                    for (int i = 0, n = consumers.size(); i < n; i++) {
                        var consumer = consumers.get(i);
                        // Setting state to ACTIVE if consumer has storage peer
                        // We will return this consumer in response to producer
                        var state = consumer.description().hasStoragePeer() ? State.ACTIVE : State.PENDING;

                        var transferId = transferDao.create(peerId, consumer.id(), channelId, state,
                            idempotencyKey + "_" + i, requestHash, tx);
                        transfers.add(new PeerAndTransfer(consumer, transferId));
                    }

                    tx.commit();
                    return new ProducerAndConsumerTransfers(prod, transfers);
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save peer description in db")
                .asRuntimeException();
        }
    }

    /**
     * Atomic consumer creation
     * If producer not found, mark consumer as not connected
     * Else mark it connected, create new active transfer and return producer with this transfer
     * All done in one transaction
     *
     * @return producer with transfer id or null if producer not found
     */
    @Nullable
    private PeerAndTransfer createConsumer(String channelId, PeerDescription peerDesc, String logPrefix, Peer.Role role,
                                           String idempotencyKey, String requestHash)
    {
        try {
            return DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var peerId = peerDesc.getPeerId();
                    var producer = peerDao.findProducer(channelId, tx);
                    peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, producer != null,
                        idempotencyKey, requestHash, tx);

                    if (producer != null) {
                        // We are assuming call cannot fail after this transaction
                        // So we can create transfer already in active state
                        var transferId = transferDao.create(producer.id(), peerId, channelId, State.ACTIVE,
                            idempotencyKey, requestHash, tx);
                        tx.commit();
                        return new PeerAndTransfer(producer, transferId);
                    }

                    tx.commit();
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save peer description in db")
                .asRuntimeException();
        }
    }

    private record PeerAndTransfer(
        Peer peer,
        String transferId
    ) {}

    private record ProducerAndConsumerTransfers(
        Peer producer,
        List<PeerAndTransfer> transfers
    ) {}
}
