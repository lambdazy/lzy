package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.db.TransferDao;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.model.Peer;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.ConcurrentModificationException;
import ai.lzy.v1.slots.LSA;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

@Singleton
public class ActionScheduler {
    private static final Logger LOG = LogManager.getLogger(ActionScheduler.class);

    private final ChannelManagerDataSource storage;
    private final TransferDao connections;
    private final SlotConnectionManager connectionManager;
    private final ChannelOperationExecutor operationExecutor;
    private final LzyServiceClient lzyServiceClient;

    public ActionScheduler(TransferDao connections, SlotConnectionManager connectionManager,
                           ChannelManagerDataSource storage, ChannelOperationExecutor operationExecutor,
                           LzyServiceClient lzyServiceClient)
    {
        this.storage = storage;
        this.connections = connections;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
        this.lzyServiceClient = lzyServiceClient;
    }

    public void runStartTransferAction(String transferId, Peer from, Peer to, @Nullable String idempotencyKey) {
        operationExecutor.schedule(() -> startTransfer(transferId, from, to, idempotencyKey), 0L, TimeUnit.SECONDS);
    }

    public void restoreActions() {
        final List<TransferDao.Transfer> transfers;
        try {
            transfers = withRetries(LOG, () -> connections.listPending(null));
        } catch (Exception e) {
            LOG.error("Cannot restore pending transmissions", e);
            throw new RuntimeException(e);
        }

        for (var transfer : transfers) {
            runStartTransferAction(transfer.id(), transfer.from(), transfer.to(), transfer.idempotencyKey());
        }
    }

    private void startTransfer(String transferId, Peer from, Peer to, @Nullable String idempotencyKey) {
        LOG.info("Connecting slot: {} to peer: {}", from, to);
        try {
            var url = from.description().getSlotPeer().getPeerUrl();

            var uri = new URI(url);

            var connection = connectionManager.getConnection(uri);

            var client = Objects.nonNull(idempotencyKey) ?
                withIdempotencyKey(connection.SlotsApi(), idempotencyKey) :
                connection.SlotsApi();

            client.startTransfer(LSA.StartTransferRequest.newBuilder()
                .setSlotId(to.id())
                .setPeer(from.description())
                .build());

            withRetries(LOG, () -> {
                try (var tx = TransactionHandle.create(storage)) {
                    var transfer = connections.get(transferId, from.channelId(), tx);
                    if (transfer != null && transfer.state().equals(TransferDao.State.PENDING)) {
                        connections.markActive(transferId, from.channelId(), idempotencyKey, tx);
                    } else {
                        throw new ConcurrentModificationException("Unexpected state of transfer", null);
                    }
                }
            });
        } catch (Exception e) {

            var reason = "(Connecting slot: %s to peer: %s): Cannot connect.".formatted(from, to);

            LOG.error(reason, e);
            try {
                withRetries(LOG, () -> lzyServiceClient.destroyChannelAndWorkflow(from.channelId(), reason,
                    idempotencyKey, null));
            } catch (Exception ex) {
                LOG.error("Cannot abort workflow", ex);
                throw new RuntimeException(ex);
            }
        }

    }
}
