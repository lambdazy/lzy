package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.TransferDao;
import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.model.Peer;
import ai.lzy.model.db.DbHelper;
import ai.lzy.v1.slots.LSA;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
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
            transfers = DbHelper.withRetries(LOG, () -> connections.listPending(null));
        } catch (Exception e) {
            LOG.error("Cannot restore pending transmissions", e);
            throw new RuntimeException(e);
        }

        for (var transfer: transfers) {
            runStartTransferAction(transfer.from(), transfer.to());
        }
    }

    private void startTransfer(Peer from, Peer to) {
        LOG.info("Connecting slot: {} to peer: {}", from, to);
        try {
            var url = from.description().getSlotPeer().getPeerUrl();

            var uri = new URI(url);

            var connection = connectionManager.getConnection(uri);

            connection.SlotsApi().startTransfer(
                LSA.StartTransferRequest.newBuilder()
                    .setSlotId(to.id())
                    .setPeer(from.description())
                    .build()
            );

            DbHelper.withRetries(LOG, () -> connections.markActive(from.id(), to.id(), null));
        } catch (Exception e) {

            var reason = "(Connecting slot: %s to peer: %s): Cannot connect.".formatted(from, to);

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
