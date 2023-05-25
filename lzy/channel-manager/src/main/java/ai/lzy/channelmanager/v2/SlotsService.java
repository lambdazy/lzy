package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LCMS.ConnectionCompletedRequest;
import ai.lzy.v1.channel.v2.LCMS.ConnectionCompletedResponse;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC.PeerDescription;
import ai.lzy.v1.common.LC.PeerDescription.SlotPeer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static ai.lzy.channelmanager.v2.Peer.Role.CONSUMER;
import static ai.lzy.channelmanager.v2.Peer.Role.PRODUCER;
import static ai.lzy.channelmanager.v2.PeerDao.Priority.BACKUP;

@Singleton
public class SlotsService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);
    private final PeerDao peerDao;
    private final ChannelManagerDataSource storage;
    private final TransmissionsDao transmissionsDao;
    private final StartTransmissionAction action;

    public SlotsService(PeerDao peerDao, ChannelManagerDataSource storage, TransmissionsDao transmissionsDao,
                        StartTransmissionAction action)
    {
        this.peerDao = peerDao;
        this.storage = storage;
        this.transmissionsDao = transmissionsDao;
        this.action = action;
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

        var logPrefix = "(Bind: {peerId: %s, channelId: %s}): ".formatted(peerId, channelId);

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

                builder.setConnectTo(prod.peerDescription());
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
                        transmissionsDao.createNotStartedTransmission(peerId, consumer.id(), tx);
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
            action.schedule(pair.getLeft(), consumer);

            if (consumer.peerDescription().hasStoragePeer()) {
                storageConsumer = consumer.peerDescription();
            }
        }

        var builder = LCMS.BindResponse.newBuilder();

        if (storageConsumer != null) {
            builder.setConnectTo(storageConsumer);
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> responseObserver) {
        try {
            DbHelper.withRetries(LOG, () -> peerDao.drop(request.getPeerId(), null));
        } catch (Exception e) {
            LOG.error("(Unbind: peerId: {}) Cannot unbind slot: ", request.getPeerId(), e);
            throw Status.INTERNAL
                .withDescription("Cannot unbind slot")
                .asRuntimeException();
        }
        responseObserver.onNext(LCMS.UnbindResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void connectionCompleted(ConnectionCompletedRequest request,
                                    StreamObserver<ConnectionCompletedResponse> responseObserver)
    {
        var logPrefix = "(ConnectionCompleted: peerId: %s, targetPeerId: %s, status: %s): ".formatted(
            request.getPeerId(),
            request.getTargetPeerId(),
            request.getStatusCase().name()
        );

        var peer = DbHelper.withRetries(LOG, () -> peerDao.get(request.getPeerId(), null));
        var target = DbHelper.withRetries(LOG, () -> peerDao.get(request.getTargetPeerId(), null));

        if (request.hasFailed()) {
            LOG.error("{} Failed with description {}", logPrefix, request.getFailed().getDescription());

            final var peerDesc = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var newPriority = peerDao.decrementPriority(request.getTargetPeerId(), tx);

                    if (newPriority < 0) {
                        LOG.error("{} Count of retries of connecting to storage peer {} is exceeded." +
                            " It will be not used for other connections", logPrefix, target.id());

                        // Retry count exceeded for consumer storage peer (cannot load data to s3), failing graph
                        if (target.peerDescription().hasStoragePeer() && target.role().equals(CONSUMER)) {
                            tx.commit();
                            LOG.error("{} Count of retries of connecting to storage peer {} is exceeded," +
                                " failing all workflow", logPrefix, request.getTargetPeerId());

                            // TODO(artolord) Destroy all here

                            throw Status.INTERNAL.asRuntimeException();
                        }
                    }

                    // Failed to load data from producer, selecting other
                    var prod = peerDao.findPriorProducer(peer.channelId(), tx);

                    // Retrying to load data to s3 with other peer
                    if (target.role().equals(CONSUMER) && target.peerDescription().hasStoragePeer()) {
                        transmissionsDao.createNotStartedTransmission(prod.id(), target.id(), tx);
                        tx.commit();
                        action.schedule(prod, target);
                        return null;
                    }
                    tx.commit();

                    if (prod == null) {
                        // This can be only if there was only one producer and connection retries count to it exceeded
                        LOG.error("{} Peer {}, which is the only one that contains data, is unavailable," +
                            " aborting workflow", logPrefix, target.id());

                        // TODO(artolord) destroy all here

                        throw Status.INTERNAL.asRuntimeException();
                    }

                    return prod.peerDescription();
                }
            });

            var builder = ConnectionCompletedResponse.newBuilder();

            if (peerDesc != null) {
                builder.setConnectTo(peerDesc);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            return;
        }

        LOG.info("{} Succeeded", logPrefix);
        if (target.peerDescription().hasStoragePeer() && target.role().equals(CONSUMER)) {
            // Data uploaded to storage, we can use it as producer
            DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    peerDao.drop(target.id(), tx);
                    peerDao.create(target.channelId(), target.peerDescription(), PRODUCER, BACKUP, false, tx);
                    tx.commit();
                }
            });
        }


    }
}
