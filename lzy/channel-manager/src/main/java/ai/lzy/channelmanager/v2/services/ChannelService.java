package ai.lzy.channelmanager.v2.services;

import ai.lzy.channelmanager.v2.db.ChannelDao;
import ai.lzy.channelmanager.v2.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.db.PeerDao;
import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class ChannelService extends LzyChannelManagerPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(ChannelService.class);

    private final ChannelDao channelDao;
    private final PeerDao peerDao;
    private final ChannelManagerDataSource storage;
    private final IdGenerator idGenerator = new RandomIdGenerator();

    public ChannelService(ChannelDao channelDao, PeerDao peerDao, ChannelManagerDataSource storage) {
        this.channelDao = channelDao;
        this.peerDao = peerDao;
        this.storage = storage;
    }

    @Override
    public void getOrCreate(GetOrCreateRequest request, StreamObserver<GetOrCreateResponse> responseObserver) {
        var logPrefix = "(GetOrCreate: {execId: %s, userId: %s}): "
            .formatted(request.getExecutionId(), request.getUserId());

        final String storageConsumerUri;
        final String storageProducerUri;

        if (request.hasConsumer()) {
            storageProducerUri = null;
            storageConsumerUri = request.getConsumer().getStorageUri();
        } else if (request.hasProducer()) {
            storageProducerUri = request.getProducer().getStorageUri();
            storageConsumerUri = null;
        } else {
            LOG.error("{} Consumer and producer not set", logPrefix);

            throw Status.INVALID_ARGUMENT
                .withDescription("Consumer or producer must be set")
                .asRuntimeException();
        }


        var peerId = idGenerator.generate("storage_peer-");
        var channelId = idGenerator.generate("channel-");

        var role = switch (request.getInitialStoragePeerCase()) {
            case PRODUCER -> Peer.Role.PRODUCER;
            case CONSUMER -> Peer.Role.CONSUMER;
            case INITIALSTORAGEPEER_NOT_SET -> throw Status.INVALID_ARGUMENT.asRuntimeException();
        };

        var peerDesc = switch (request.getInitialStoragePeerCase()) {
            case PRODUCER -> PeerDescription.newBuilder()
                .setPeerId(peerId)
                .setStoragePeer(request.getProducer())
                .build();
            case CONSUMER -> PeerDescription.newBuilder()
                .setPeerId(peerId)
                .setStoragePeer(request.getConsumer())
                .build();
            case INITIALSTORAGEPEER_NOT_SET -> throw Status.INVALID_ARGUMENT.asRuntimeException();
        };

        try {
            var res = DbHelper.withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var channel = channelDao.find(request.getUserId(), request.getExecutionId(), storageProducerUri,
                        storageConsumerUri, tx);

                    if (channel != null) {
                        // Channel already exists, returning it
                        return channel;
                    }

                    // Creating channel and peer
                    channel = channelDao.create(channelId, request.getUserId(), request.getExecutionId(),
                        request.getWorkflowName(), request.getScheme(), storageProducerUri, storageConsumerUri, tx);
                    peerDao.create(channelId, peerDesc, role, PeerDao.Priority.BACKUP, false, tx);

                    tx.commit();
                    return channel;
                }
            });

            responseObserver.onNext(GetOrCreateResponse.newBuilder()
                .setChannelId(res.id())
                .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("{} Cannot create channel in db: ", logPrefix, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Cannot create channel in db")
                .asRuntimeException());
        }
    }

    @Override
    public void destroy(LCMPS.DestroyRequest request, StreamObserver<LCMPS.DestroyResponse> responseObserver) {
        var logPrefix = "(Destroy: {channelId: %s}): ".formatted(request.getChannelId());
        LOG.info("{} Destroying channel with reason {}", logPrefix, request.getReason());

        try {
            DbHelper.withRetries(LOG, () -> channelDao.drop(request.getChannelId(), null));
        } catch (Exception e) {
            LOG.error("{} Cannot destroy channel in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot destroy channel in db")
                .asRuntimeException();
        }

        responseObserver.onNext(LCMPS.DestroyResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void destroyAll(LCMPS.DestroyAllRequest request, StreamObserver<LCMPS.DestroyAllResponse> responseObserver) {
        var logPrefix = "(DestroyAll: {executionId: %s}): ".formatted(request.getExecutionId());
        LOG.info("{} Destroying all channels with reason {}", logPrefix, request.getReason());

        try {
            DbHelper.withRetries(LOG, () -> channelDao.dropAll(request.getExecutionId(), null));
        } catch (Exception e) {
            LOG.error("{} Cannot destroy channels in db: ", logPrefix, e);
            throw Status.INTERNAL
                .withDescription("Cannot destroy channels in db")
                .asRuntimeException();
        }

        responseObserver.onNext(LCMPS.DestroyAllResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
