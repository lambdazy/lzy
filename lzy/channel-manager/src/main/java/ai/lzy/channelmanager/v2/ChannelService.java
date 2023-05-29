package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LCMPS.GetOrCreateRequest;
import ai.lzy.v1.channel.v2.LCMPS.GetOrCreateResponse;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase;
import ai.lzy.v1.common.LC.PeerDescription;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

@Singleton
public class ChannelService extends LzyChannelManagerPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(ChannelService.class);

    private final ChannelDao channelDao;
    private final PeerDao peerDao;
    private final ChannelManagerDataSource storage;

    public ChannelService(ChannelDao channelDao, PeerDao peerDao, ChannelManagerDataSource storage) {
        this.channelDao = channelDao;
        this.peerDao = peerDao;
        this.storage = storage;
    }

    @Override
    public void getOrCreate(GetOrCreateRequest request, StreamObserver<GetOrCreateResponse> responseObserver) {
        var logPrefix = "(GetOrCreate: {execId: %s, userId: %s}): "
            .formatted(request.getExecutionId(), request.getUserId());

        final String consumerUri;
        final String producerUri;

        if (request.hasConsumer()) {
            producerUri = null;
            consumerUri = request.getConsumer().getStorageUri();
        } else if (request.hasProducer()) {
            producerUri = request.getProducer().getStorageUri();
            consumerUri = null;
        } else {
            LOG.error("{} Consumer and producer not set", logPrefix);

            throw Status.INVALID_ARGUMENT
                .withDescription("Consumer or producer must be set")
                .asRuntimeException();
        }

        final Channel channel;

        try {
            channel = DbHelper.withRetries(LOG,
                () -> channelDao.find(request.getUserId(), request.getExecutionId(), producerUri, consumerUri, null));
        } catch (Exception e) {
            LOG.error("{} Cannot find channel in db: ", logPrefix, e);
            throw Status.INTERNAL.asRuntimeException();
        }

        if (channel != null) {
            responseObserver.onNext(GetOrCreateResponse.newBuilder()
                .setChannelId(channel.id())
                .build());
            responseObserver.onCompleted();
            return;
        }

        var id = UUID.randomUUID().toString();
        var pair = switch (request.getInitialStoragePeerCase()) {
            case PRODUCER -> Pair.of(
                Peer.Role.PRODUCER,
                PeerDescription.newBuilder()
                    .setStoragePeer(request.getProducer())
                    .build()
            );
            case CONSUMER -> Pair.of(
                Peer.Role.CONSUMER,
                PeerDescription.newBuilder()
                    .setStoragePeer(request.getConsumer())
                    .build()
            );
            case INITIALSTORAGEPEER_NOT_SET -> throw Status.INVALID_ARGUMENT.asRuntimeException();
        };

        try {
            DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {

                    channelDao.create(id, request.getUserId(), request.getExecutionId(), request.getScheme(),
                        producerUri, consumerUri, tx);

                    peerDao.create(id, pair.getRight(), pair.getLeft(), PeerDao.Priority.BACKUP, false, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            LOG.error("{} Cannot save channel in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot save channel in db")
                .asRuntimeException();
        }

        responseObserver.onNext(GetOrCreateResponse.newBuilder()
            .setChannelId(id)
            .build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(LCMPS.StatusRequest request, StreamObserver<LCMPS.StatusResponse> responseObserver) {
        super.status(request, responseObserver);
    }

    @Override
    public void destroy(LCMPS.DestroyRequest request, StreamObserver<LCMPS.DestroyResponse> responseObserver) {
        var logPrefix = "(Destroy: {channelId: %s}): ".formatted(request.getChannelId());

    }

    @Override
    public void destroyAll(LCMPS.DestroyAllRequest request, StreamObserver<LCMPS.DestroyAllResponse> responseObserver) {
        super.destroyAll(request, responseObserver);
    }

    @Override
    public void list(LCMPS.ListRequest request, StreamObserver<LCMPS.ListResponse> responseObserver) {
        super.list(request, responseObserver);
    }
}
