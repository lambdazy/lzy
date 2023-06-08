package ai.lzy.channelmanager.v2.services;

import ai.lzy.channelmanager.access.IamAccessManager;
import ai.lzy.channelmanager.v2.ActionScheduler;
import ai.lzy.channelmanager.v2.LzyServiceClient;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.db.PeerDao;
import ai.lzy.channelmanager.v2.db.TransferDao;
import ai.lzy.channelmanager.v2.db.TransferDao.State;
import ai.lzy.channelmanager.v2.db.TransferDao.Transfer;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LCMS.*;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ai.lzy.channelmanager.v2.db.PeerDao.Priority.BACKUP;
import static ai.lzy.channelmanager.v2.model.Peer.Role.CONSUMER;
import static ai.lzy.channelmanager.v2.model.Peer.Role.PRODUCER;
import static ai.lzy.iam.resources.AuthPermission.WORKFLOW_RUN;

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

    private final IdGenerator idGenerator = new RandomIdGenerator();

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
        // TODO(artolord) add idempotency

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
            case PRODUCER -> Peer.Role.PRODUCER;
        };

        if (role == CONSUMER) {
            final PeerAndTransfer producerAndTransfer = createConsumer(channelId, peerId, peerDesc, logPrefix, role);

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

        final ProducerAndConsumerTransfers res = createProducer(channelId, peerId, peerDesc, logPrefix, role);

        // Finding storage consumer
        PeerAndTransfer storageConsumer = null;
        for (var consumer: res.transfers) {
            if (consumer.peer().description().hasStoragePeer()) {
                storageConsumer = consumer;
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
        for (var consumer: res.transfers) {
            if (!consumer.peer().description().hasStoragePeer()) {
                action.runStartTransferAction(res.producer, consumer.peer);
            }
        }
    }

    /**
     * Atomic operation to create producer
     * If there are not connected consumers, then this consumers will be marked as connected
     *  and transfers will be created.
     * @return pair of producer and list of consumer transfers
     */
    private ProducerAndConsumerTransfers createProducer(String channelId, String peerId, PeerDescription peerDesc,
                                                        String logPrefix, Peer.Role role)
    {
        final ProducerAndConsumerTransfers result;
        try {
            result = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var prod = peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, false, tx);

                    final var consumers = peerDao.markConsumersAsConnected(channelId, tx);

                    final var transfers = new ArrayList<PeerAndTransfer>();

                    for (var consumer : consumers) {
                        var transferId = idGenerator.generate("transfer-");

                        // Setting state to ACTIVE if consumer has storage peer
                        // We will return this consumer in response to producer
                        var state = consumer.description().hasStoragePeer() ? State.ACTIVE : State.PENDING;

                        transferDao.create(transferId, peerId, consumer.id(), channelId, state, tx);
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
        return result;
    }

    /**
     * Atomic consumer creation
     * If producer not found, mark consumer as not connected
     * Else mark it connected, create new active transfer and return producer with this transfer
     * All done in one transaction
     * @return producer with transfer id or null if producer not found
     */
    @Nullable
    private PeerAndTransfer createConsumer(String channelId, String peerId, PeerDescription peerDesc, String logPrefix,
                                           Peer.Role role)
    {
        final PeerAndTransfer result;
        try {
            result = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var producer = peerDao.findProducer(channelId, tx);
                    peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, producer != null, tx);

                    String transferId = null;

                    if (producer != null) {
                        transferId = idGenerator.generate("transfer-");

                        // We are assuming call cannot fail after this transaction
                        // So we can create transfer already in active state
                        transferDao.create(
                            transferId, producer.id(), peerId, channelId, State.ACTIVE, tx);
                    }

                    tx.commit();
                    return producer == null ? null : new PeerAndTransfer(producer, transferId);
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save peer description in db")
                .asRuntimeException();
        }
        return result;
    }

    private record PeerAndTransfer(Peer peer, String transferId) {}

    private record ProducerAndConsumerTransfers(Peer producer, List<PeerAndTransfer> transfers) {}

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

        final PeerAndTransfer resp;

        try {
            resp = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    Transfer transfer = getAndValidateTransfer(request.getTransferId(), channel, logPrefix, tx);

                    LOG.error("{} Failed with description {}", logPrefix, request.getDescription());
                    transferDao.markFailed(request.getTransferId(), channel.id(), request.getDescription(), tx);

                    if (transfer.to().description().hasStoragePeer()) {
                        // Failed to send data to storage peer, failing whole channel
                        LOG.error("{} Uploading data to storage {} for channel failed", logPrefix,
                            transfer.to().description().getStoragePeer().getStorageUri());

                        lzyServiceClient.destroyChannelAndWorkflow(channel.id(),
                            "Uploading data to storage failed", tx);
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

                    // Failed to load data from producer, selecting other
                    var newProducer = peerDao.findProducer(transfer.channelId(), tx);
                    if (newProducer == null) {
                        // This can be only if there was only one producer and connection retries count to it exceeded
                        LOG.error("{} No more producers in channel, while there are consumers", logPrefix);

                        lzyServiceClient.destroyChannelAndWorkflow(channel.id(),
                            "No more producers in channel, while there are consumers", tx);
                        tx.commit();
                        throw Status.INTERNAL
                            .withDescription("No more producers in channel, while there are consumers")
                            .asRuntimeException();
                    }

                    // Creating new transfer with found producer
                    var newTransferId = idGenerator.generate("transfer-");
                    transferDao.create(newTransferId, newProducer.id(), transfer.from().id(), channel.id(),
                        State.ACTIVE, tx);
                    tx.commit();
                    return new PeerAndTransfer(newProducer, newTransferId);
                }
            });
        }  catch (StatusRuntimeException e) {
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

    private Transfer getAndValidateTransfer(String id, Channel channel, String logPrefix,
                                            @Nullable TransactionHandle tx) throws SQLException
    {
        var transfer = transferDao.get(id, channel.id(), tx);

        if (transfer == null) {
            LOG.error("{} Cannot find transfer in db", logPrefix);
            throw Status.NOT_FOUND
                .withDescription("Cannot find transfer in db")
                .asRuntimeException();
        }

        if (transfer.state() != State.ACTIVE) {
            LOG.error("{} Transfer is not active", logPrefix);
            throw Status.FAILED_PRECONDITION
                .withDescription("Transfer is not active")
                .asRuntimeException();
        }

        return transfer;
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

        try {
            DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var transfer = getAndValidateTransfer(request.getTransferId(), channel, logPrefix, tx);

                    LOG.info("{} Succeeded", logPrefix);
                    transferDao.markCompleted(request.getTransferId(), channel.id(), tx);

                    if (transfer.to().description().hasStoragePeer()) {
                        // Data uploaded to storage, we can use it as producer

                        peerDao.drop(transfer.to().id(), channel.id(), tx);
                        peerDao.create(channel.id(), transfer.to().description(), PRODUCER, BACKUP, false, tx);
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

        for (var chan: channels) {
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

        var hasAccess = accessManager.checkAccess(subjId, channel.userId(), channel.workflowName(),
            WORKFLOW_RUN);

        if (!hasAccess) {
            LOG.error("{}: Permission denied (executionId: {}, userId: {}, subjId: {})", callName,
                channel.executionId(), channel.userId(), subjId);
            throw Status.PERMISSION_DENIED.asRuntimeException();
        }
        return channel;
    }
}
