package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.v2.db.TransferDao;
import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.model.db.DbHelper;
import ai.lzy.v1.slots.v2.LSA;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Singleton
public class ActionScheduler {
    private static final Logger LOG = LogManager.getLogger(ActionScheduler.class);

    private final TransferDao connections;

    private final SlotConnectionManager connectionManager;
    private final ChannelOperationExecutor operationExecutor;
    private final LzyServiceClient lzyServiceClient;

    public ActionScheduler(TransferDao connections, SlotConnectionManager connectionManager,
                           ChannelOperationExecutor operationExecutor, LzyServiceClient lzyServiceClient)
    {
        this.connections = connections;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
        this.lzyServiceClient = lzyServiceClient;
    }

    public void runStartTransferAction(Peer from, Peer to) {
        operationExecutor.schedule(() -> startTransfer(from, to), 0L, TimeUnit.SECONDS);
    }

    public void restoreActions() {
        final List<TransferDao.Transfer> transfers;
        try {
            transfers = DbHelper.withRetries(LOG, () -> connections.listPendingTransmissions(null));
        } catch (Exception e) {
            LOG.error("Cannot restore pending transmissions", e);
            throw new RuntimeException(e);
        }

        for (var transmission: transfers) {
            runStartTransferAction(transmission.from(), transmission.to());
        }
    }

    private void startTransfer(Peer from, Peer to) {
        LOG.info("Connecting from: {} to to: {}", from, to);
        try {
            var url = from.peerDescription().getSlotPeer().getPeerUrl();

            var uri = new URI(url);

            var connection = connectionManager.getConnection(uri);

            connection.v2SlotsApi().startTransfer(
                LSA.StartTransferRequest.newBuilder()
                    .setSlotId(to.id())
                    .setPeer(from.peerDescription())
                    .build()
            );

            DbHelper.withRetries(LOG, () -> connections.dropPendingTransfer(from.id(), to.id(), null));
        } catch (Exception e) {

            var reason = "(Connecting slot: %s to peer: %s): Cannot connect. errId: %s".formatted(from, to,
                UUID.randomUUID().toString());

            LOG.error(reason, e);
            try {
                DbHelper.withRetries(LOG, () -> lzyServiceClient.destroyChannelAndWorkflow(from.channelId(), reason,
                    null));
            } catch (Exception ex) {
                LOG.error("Cannot abort workflow", ex);
                throw new RuntimeException(ex);
            }
        }

    }
}
