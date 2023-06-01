package ai.lzy.channelmanager.v2.services;

import ai.lzy.channelmanager.access.IamAccessManager;
import ai.lzy.channelmanager.v2.ActionScheduler;
import ai.lzy.channelmanager.v2.Utils;
import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.db.PeerDao;
import ai.lzy.channelmanager.v2.db.TransferDao;
import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Peer;
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
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

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
    private final Utils utils;
    private final IamAccessManager accessManager;

    public SlotsService(PeerDao peerDao, ChannelDao channelDao, ChannelManagerDataSource storage,
                        TransferDao transferDao, ActionScheduler action, Utils utils,
                        IamAccessManager accessManager)
    {
        this.peerDao = peerDao;
        this.channelDao = channelDao;
        this.storage = storage;
        this.transferDao = transferDao;
        this.action = action;
        this.utils = utils;
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

        if (role.equals(CONSUMER)) {

            // Atomic consumer creation
            // If producer not found, mark consumer as not connected
            // Else mark it completed and return producer
            final Peer prod;
            try {
                prod = DbHelper.withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        var producer = peerDao.findPriorProducer(channelId, tx);
                        peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, producer != null, tx);
                        tx.commit();

                        return producer;
                    }
                });
            } catch (Exception e) {
                LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
                throw Status.INTERNAL
                    .withDescription("Cannot save peer description in db")
                    .asRuntimeException();
            }

            var builder = LCMS.BindResponse.newBuilder();

            if (prod != null) {
                LOG.info("{} Connected to producer(peerId: {})", logPrefix, prod.id());

                builder.setPeer(prod.peerDescription());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            return;
        }

        final Pair<Peer, List<Peer>> pair;
        try {
            pair = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var prod = peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, false, tx);

                    final var consumers = peerDao.markConsumersAsConnected(channelId, tx);

                    for (var consumer : consumers) {
                        transferDao.createPendingTransmission(peerId, consumer.id(), tx);
                    }

                    tx.commit();
                    return Pair.of(prod, consumers);
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save peer description in db")
                .asRuntimeException();
        }

        PeerDescription storageConsumer = null;

        for (var consumer: pair.getRight()) {
            action.scheduleStartTransferAction(pair.getLeft(), consumer);

            if (consumer.peerDescription().hasStoragePeer()) {
                storageConsumer = consumer.peerDescription();
            }
        }

        var builder = LCMS.BindResponse.newBuilder();

        if (storageConsumer != null) {
            builder.setPeer(storageConsumer);
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
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
                    if (!transferDao.hasPendingTransfers(request.getPeerId(), tx)) {
                        peerDao.drop(request.getPeerId(), tx);
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
        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "TransmissionCompleted");

        var logPrefix = ("(TransmissionFailed: {loaderPeerId: %s, targetPeerId: %s,  channelId: %s, userId: %s," +
            " workflowName: %s, execId: %s}): ").formatted(request.getSlotId(), request.getPeerId(),
            request.getChannelId(), channel.userId(), channel.workflowName(), channel.executionId()
        );

        final Peer slot;
        final Peer peer;
        try {
            slot = DbHelper.withRetries(LOG, () -> peerDao.get(request.getSlotId(), null));
            peer = DbHelper.withRetries(LOG, () -> peerDao.get(request.getPeerId(), null));
        } catch (Exception e) {
            LOG.error("{} Cannot get slot and peer from db", logPrefix, e);
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (slot == null || peer == null) {
            LOG.error("{} Slot or peer not found in db", logPrefix);
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            return;
        }

        LOG.error("{} Failed with description {}", logPrefix, request.getDescription());

        if (peer.role().equals(CONSUMER) && peer.peerDescription().hasStoragePeer()) {
            // Loading data to storage failed, failing all execution

            dropChannel("Uploading data to storage %s for channel failed".formatted(
                peer.peerDescription().getStoragePeer().getStorageUri()
            ), logPrefix, null, slot.channelId());

            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        final Peer newProducer;
        try {
            newProducer = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var newPriority = peerDao.decrementPriority(peer.id(), tx);

                    if (newPriority < 0) {
                        LOG.error("{} Count of retries of connecting to peer {} is exceeded." +
                            " It will be not used for other connections", logPrefix, peer.id());
                    }

                    // Failed to load data from producer, selecting other
                    var prod = peerDao.findPriorProducer(slot.channelId(), tx);
                    tx.commit();

                    return prod;
                }
            });
        } catch (Exception e) {
            dropChannel("Cannot get peer from db", logPrefix, e, slot.channelId());
            throw Status.INTERNAL.asRuntimeException();
        }

        if (newProducer == null) {
            // This can be only if there was only one producer and connection retries count to it exceeded
            dropChannel("No more producers in channel, while there are consumers",
                logPrefix, null, slot.channelId());

            throw Status.INTERNAL.asRuntimeException();
        }

        responseObserver.onNext(TransferFailedResponse.newBuilder()
            .setNewPeer(newProducer.peerDescription())
            .build()
        );

        responseObserver.onCompleted();
    }

    @Override
    public void transferCompleted(TransferCompletedRequest request,
                                  StreamObserver<TransferCompletedResponse> responseObserver)
    {
        final Channel channel = getChannelAndCheckAccess(request.getChannelId(), "TransmissionCompleted");

        var logPrefix = ("(TransmissionCompleted: {loaderPeerId: %s, targetPeerId: %s,  channelId: %s, userId: %s," +
            " workflowName: %s, execId: %s}): ").formatted(request.getSlotId(), request.getPeerId(),
            request.getChannelId(), channel.userId(), channel.workflowName(), channel.executionId()
        );

        final Peer slot;
        final Peer peer;
        try {
            slot = DbHelper.withRetries(LOG, () -> peerDao.get(request.getSlotId(), null));
            peer = DbHelper.withRetries(LOG, () -> peerDao.get(request.getPeerId(), null));
        } catch (Exception e) {
            LOG.error("{} Cannot get slot and peer from db", logPrefix, e);
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (slot == null || peer == null) {
            LOG.error("{} Loader or peer not found in db", logPrefix);
            responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
            return;
        }

        LOG.info("{} Succeeded", logPrefix);
        if (peer.peerDescription().hasStoragePeer() && peer.role().equals(CONSUMER)) {
            // Data uploaded to storage, we can use it as producer
            try {
                DbHelper.withRetries(LOG, () -> {
                    try (var tx = TransactionHandle.create(storage)) {
                        peerDao.drop(peer.id(), tx);
                        peerDao.create(peer.channelId(), peer.peerDescription(), PRODUCER, BACKUP, false, tx);
                        tx.commit();
                    }
                });
            } catch (Exception e) {
                dropChannel("Cannot recreate storage peer as producer", logPrefix, e, slot.channelId());

                throw Status.INTERNAL.asRuntimeException();
            }
        }

        responseObserver.onNext(TransferCompletedResponse.newBuilder().build());
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
            LOG.error("{}: Permission denied (executionId: {}, userId: {}, subjId: {}", callName,
                channel.executionId(), channel.userId(), subjId);
            throw Status.PERMISSION_DENIED.asRuntimeException();
        }
        return channel;
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

    private void dropChannel(String reason, String logPrefix, Exception e, String channelId) {
        final var r = "%s %s. channelId: %s, errId: %s".formatted(logPrefix, reason, channelId,
            UUID.randomUUID().toString());

        if (e != null) {
            LOG.error(r, e);
        } else {
            LOG.error(r);
        }

        try {
            DbHelper.withRetries(LOG, () -> utils.destroyChannelAndWorkflow(channelId, r, null));

        } catch (Exception ex) {
            LOG.error("{} Cannot abort workflow for channel {}: ", logPrefix, channelId, ex);
        }
    }
}
