package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.channelmanager.v2.db.TransmissionsDao;
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
public class StartTransmissionAction {
    private static final Logger LOG = LogManager.getLogger(StartTransmissionAction.class);

    private final TransmissionsDao connections;

    private final SlotConnectionManager connectionManager;
    private final ChannelOperationExecutor operationExecutor;
    private final Utils utils;

    public StartTransmissionAction(TransmissionsDao connections, SlotConnectionManager connectionManager,
                                   ChannelOperationExecutor operationExecutor, Utils utils)
    {
        this.connections = connections;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
        this.utils = utils;
    }

    public void schedule(Peer loader, Peer target) {
        operationExecutor.schedule(() -> run(loader, target), 0L, TimeUnit.SECONDS);
    }

    public void restoreActions() {
        final List<TransmissionsDao.Transmission> transmissions;
        try {
            transmissions = DbHelper.withRetries(LOG, () -> connections.listPendingTransmissions(null));
        } catch (Exception e) {
            LOG.error("Cannot restore pending transmissions", e);
            throw new RuntimeException(e);
        }

        for (var transmission: transmissions) {
            schedule(transmission.loader(), transmission.target());
        }
    }

    private void run(Peer loader, Peer target) {

        try {
            var url = loader.peerDescription().getSlotPeer().getPeerUrl();

            var uri = new URI(url);

            var connection = connectionManager.getConnection(uri);

            connection.v2SlotsApi().startTransmission(
                LSA.StartTransmissionRequest.newBuilder()
                    .setLoaderPeerId(target.id())
                    .setTarget(loader.peerDescription())
                    .build()
            );

            DbHelper.withRetries(LOG, () -> connections.dropPendingTransmission(loader.id(), target.id(), null));
        } catch (Exception e) {

            var reason = "(Connecting loader: %s to target: %s): Cannot connect. errId: %s".formatted(loader, target,
                UUID.randomUUID().toString());

            LOG.error(reason, e);
            try {
                DbHelper.withRetries(LOG, () -> utils.destroyChannelAndWorkflow(loader.channelId(), reason, null));
            } catch (Exception ex) {
                LOG.error("Cannot abort workflow", ex);
                throw new RuntimeException(ex);
            }
        }

    }
}
