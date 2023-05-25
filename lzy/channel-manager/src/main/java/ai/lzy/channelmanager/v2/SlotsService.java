package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.common.LC.PeerDescription;
import ai.lzy.v1.common.LC.PeerDescription.SlotPeer;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class SlotsService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {
    private static final Logger LOG = LogManager.getLogger(SlotsService.class);
    private final PeerDao peerDao;
    private final ChannelManagerDataSource storage;

    public SlotsService(PeerDao peerDao, ChannelManagerDataSource storage) {
        this.peerDao = peerDao;
        this.storage = storage;
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

        var logPrefix = "(Bind: {peerId: %s, channelId: %s}): ".formatted(peerId, channelId);

        var role = switch (request.getRole()) {
            case UNSPECIFIED, UNRECOGNIZED -> {
                LOG.error("{} Cannot determine role", logPrefix);

                throw Status.INVALID_ARGUMENT
                    .withDescription("Role of peer not set or unsupported")
                    .asRuntimeException();
            }
            case CONSUMER -> Peer.Role.CONSUMER;
            case PRODUCER -> Peer.Role.PRODUCER;
        };

        if (role.equals(Peer.Role.CONSUMER)) {

            // Atomic consumer creation
            // If producer not found, mark consumer as not completed
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

        try {
            DbHelper.withRetries(LOG,
                () -> peerDao.create(channelId, peerDesc, role, PeerDao.Priority.PRIMARY, false, null));
        } catch (Exception e) {
            LOG.error("{} Cannot save peer description in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save peer description in db")
                .asRuntimeException();
        }

        try (var tx = TransactionHandle.create(storage)) {
            final var consumers = peerDao.markConsumersAsConnected(channelId, tx);

            for (var consumer : consumers) {
                // TODO(artolord) maybe connection must be done in separate persistent job
            }
        }


    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> responseObserver) {
        super.unbind(request, responseObserver);
    }

    @Override
    public void connectionCompleted(LCMS.ConnectionCompletedRequest request, StreamObserver<LCMS.ConnectionCompletedResponse> responseObserver) {
        super.connectionCompleted(request, responseObserver);
    }
}
